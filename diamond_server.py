#!/usr/bin/env python3
"""
DIAMOND HR Prop Board — Live Data Server
-----------------------------------------
Run:  python3 diamond_server.py
Then: open http://localhost:8765 in your browser

Fetches on every page load:
  • Today's MLB schedule + probable pitchers (MLB Stats API)
  • Confirmed lineups (MLB Stats API)
  • Game-time weather per stadium (Open-Meteo, free, no key)
  • Statcast batting stats — barrel%, EV, hard hit%, xwOBA (Baseball Savant)
  • Injury list (MLB Stats API injury endpoint)

All data is cached for 15 minutes so rapid refreshes don't re-fetch.
"""

import json, time, threading, traceback, difflib, os, concurrent.futures
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.parse import urlparse, parse_qs

# ── CONFIG ───────────────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 8765))
HOST = os.environ.get("HOST", "0.0.0.0")
CACHE_TTL    = 900   # 15 minutes — general data
ROSTER_TTL   = 600   # 10 minutes — rosters + injuries
SPLITS_TTL   = 3600  # 1 hour — splits rarely change intraday
API_TIMEOUT  = 10    # seconds — hard cap on all external API calls
USER_AGENT   = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
_SAVANT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://baseballsavant.mlb.com/",
    "Connection": "keep-alive",
}

# Optional: set ODDS_API_KEY env var for game totals (free at the-odds-api.com)
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

# Full MLB team name → our abbreviation (for Odds API matching)
ODDS_TEAM_MAP = {
    "yankees":"NYY","mets":"NYM","red sox":"BOS","dodgers":"LAD","angels":"LAA",
    "astros":"HOU","braves":"ATL","cubs":"CHC","white sox":"CWS","guardians":"CLE",
    "tigers":"DET","royals":"KC","twins":"MIN","padres":"SD","giants":"SF",
    "cardinals":"STL","brewers":"MIL","reds":"CIN","pirates":"PIT","phillies":"PHI",
    "nationals":"WSH","marlins":"MIA","orioles":"BAL","rays":"TBR","blue jays":"TOR",
    "rangers":"TEX","mariners":"SEA","athletics":"ATH","rockies":"COL",
}

# MLB official team IDs (active roster endpoint)
MLB_TEAM_IDS = {
    "ARI": 109, "ATL": 144, "BAL": 110, "BOS": 111, "CHC": 112,
    "CIN": 113, "CLE": 114, "COL": 115, "CWS": 145, "DET": 116,
    "HOU": 117, "KC":  118, "LAA": 108, "LAD": 119, "MIA": 146,
    "MIL": 158, "MIN": 142, "NYM": 121, "NYY": 147, "ATH": 133,
    "PHI": 143, "PIT": 134, "SD":  135, "SEA": 136, "SF":  137,
    "STL": 138, "TBR": 139, "TEX": 140, "TOR": 141, "WSH": 120,
}

# Stadium coordinates for weather (home team → lat/lng)
# Stadium coordinates for weather (home team → lat/lng)
STADIUM_COORDS = {
    "ARI": (33.446, -112.067, "Chase Field"),
    "ATL": (33.891, -84.468,  "Truist Park"),
    "BAL": (39.284, -76.622,  "Oriole Park"),
    "BOS": (42.347, -71.097,  "Fenway Park"),
    "CHC": (41.948, -87.655,  "Wrigley Field"),
    "CIN": (39.097, -84.507,  "Great American BP"),
    "CLE": (41.496, -81.685,  "Progressive Field"),
    "COL": (39.756, -104.994, "Coors Field"),
    "CWS": (41.830, -87.634,  "Rate Field"),
    "DET": (42.339, -83.049,  "Comerica Park"),
    "HOU": (29.757, -95.355,  "Daikin Park"),
    "KC":  (39.051, -94.480,  "Kauffman Stadium"),
    "LAA": (33.800, -117.883, "Angel Stadium"),
    "LAD": (34.074, -118.240, "Dodger Stadium"),
    "MIA": (25.778, -80.220,  "loanDepot Park"),
    "MIL": (43.028, -87.971,  "American Family Field"),
    "MIN": (44.981, -93.278,  "Target Field"),
    "NYM": (40.757, -73.846,  "Citi Field"),
    "NYY": (40.829, -73.926,  "Yankee Stadium"),
    "OAK": (38.583, -121.499, "Sutter Health Park"),
    "ATH": (38.583, -121.499, "Sutter Health Park"),
    "PHI": (39.906, -75.166,  "Citizens Bank Park"),
    "PIT": (40.447, -80.006,  "PNC Park"),
    "SD":  (32.707, -117.157, "Petco Park"),
    "SEA": (47.591, -122.332, "T-Mobile Park"),
    "SF":  (37.779, -122.389, "Oracle Park"),
    "STL": (38.623, -90.193,  "Busch Stadium"),
    "TBR": (27.768, -82.653,  "Tropicana Field"),
    "TEX": (32.747, -97.083,  "Globe Life Field"),
    "TOR": (43.641, -79.389,  "Rogers Centre"),
    "WSH": (38.873, -77.007,  "Nationals Park"),
}

# Park factors (2026 season, Statcast-adjusted)
PARK_FACTORS = {
    "ARI":114,"ATL":96,"BAL":106,"BOS":102,"CHC":95,"CIN":118,"CLE":97,
    "COL":150,"CWS":93,"DET":101,"HOU":107,"KC":97,"LAA":101,"LAD":96,
    "MIA":92,"MIL":108,"MIN":95,"NYM":103,"NYY":110,"OAK":112,"ATH":112,
    "PHI":114,"PIT":90,"SD":94,"SEA":98,"SF":99,"STL":104,"TBR":93,
    "TEX":109,"TOR":105,"WSH":95
}

# Known domes / retractable roofs (weather excluded when closed)
DOMES = {"TBR","HOU","MIL","TEX","ARI","TOR","MIA","SEA"}

# Pull-side park bonuses — extra HR boost for pull hitters at short-porch parks
# L = bonus for left-handed pull hitters (pull to RF), R = right-handed (pull to LF)
PULL_BONUS = {
    "NYY": {"L":12,"R": 0},  # 314ft RF short porch
    "BOS": {"L":10,"R": 0},  # 302ft Pesky Pole
    "HOU": {"L": 8,"R": 0},  # Crawford Boxes LF → actually short LF, benefits RHB
    "PHI": {"L": 5,"R": 3},
    "CIN": {"L": 4,"R": 6},  # Great American BP — short LF
    "COL": {"L": 5,"R": 5},  # Altitude benefits all
    "TEX": {"L": 4,"R": 3},
    "STL": {"L": 2,"R": 4},
    "BAL": {"L": 2,"R": 3},
    "MIL": {"L": 2,"R": 4},
    "LAD": {"L": 2,"R": 5},  # 330ft LF
    "ATL": {"L": 0,"R": 4},
}

# Lineup position bonus — cleanup hitters see better pitches
LINEUP_BONUS = {1:1,2:2,3:4,4:6,5:3,6:1,7:0,8:-1,9:-2}

# Umpire zone tendencies: positive = hitter-friendly (tight zone → more FBs in zone)
# negative = pitcher-friendly (liberal zone → pitchers expand early in count)
UMP_ZONES = {
    "CB Bucknor":       -2.0, "Angel Hernandez":  -1.8, "Joe West":         -1.2,
    "Laz Diaz":         -1.5, "Doug Eddings":     -1.0, "Gary Cederstrom":  -0.8,
    "Ron Kulpa":         1.5, "Quinn Wolcott":     1.2, "Marvin Hudson":     1.0,
    "Mark Carlson":      0.8, "John Tumpane":      0.6, "Jim Reynolds":     -0.5,
    "Stu Scheurwater":   1.0, "Adam Hamari":       0.7, "Chris Guccione":   -0.6,
}

# ── TEAM ABBREVIATION MAP (MLB API → our codes) ──────────────────────────────
TEAM_MAP = {
    "TOR":"TOR","BAL":"BAL","NYY":"NYY","TB":"TBR","TBR":"TBR","BOS":"BOS",
    "CLE":"CLE","DET":"DET","CWS":"CWS","MIN":"MIN","KC":"KC","HOU":"HOU",
    "LAA":"LAA","OAK":"ATH","ATH":"ATH","SEA":"SEA","TEX":"TEX","NYM":"NYM",
    "PHI":"PHI","ATL":"ATL","MIA":"MIA","WSH":"WSH","CHC":"CHC","STL":"STL",
    "MIL":"MIL","CIN":"CIN","PIT":"PIT","SF":"SF","COL":"COL","LAD":"LAD",
    "ARI":"ARI","SD":"SD",
}

# ── IN-MEMORY CACHE ──────────────────────────────────────────────────────────
_cache = {}
_cache_lock = threading.Lock()

def cache_get(key, ttl=None):
    with _cache_lock:
        entry = _cache.get(key)
        effective_ttl = ttl if ttl is not None else CACHE_TTL
        if entry and time.time() - entry["ts"] < effective_ttl:
            return entry["data"]
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {"ts": time.time(), "data": data}

# ── HTTP HELPER ──────────────────────────────────────────────────────────────
def fetch(url, timeout=None, headers=None):
    """Fetch JSON from url. Follows HTTP 301/302/307/308 redirects manually."""
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    req = Request(url, headers=h)
    try:
        with urlopen(req, timeout=timeout or API_TIMEOUT) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        # urllib doesn't follow 308 — do it manually
        import urllib.error
        if isinstance(e, urllib.error.HTTPError) and e.code in (301, 302, 307, 308):
            loc = e.headers.get("Location", "")
            if loc:
                return fetch(loc, timeout=timeout, headers=headers)
        raise

def fetch_savant(url, timeout=15):
    """Fetch Baseball Savant CSV/JSON with full browser headers to bypass 403."""
    req = Request(url, headers=_SAVANT_HEADERS)
    with urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8")

def _safe_float(v, default=0.0):
    """Parse MLB API stat strings: '.295', '1.050', '0.912', etc."""
    try:
        s = str(v or "").strip()
        if not s or s in ("-", "null", "None", ".---", "-.--", ""):
            return default
        if s.startswith("."):
            s = "0" + s
        return float(s)
    except Exception:
        return default

def fetch_40man_roster():
    """Shared 40-man roster fetch — cached so injuries + rosters don't double-fetch."""
    cached = cache_get("_40man", ttl=ROSTER_TTL)
    if cached: return cached
    try:
        data = fetch("https://statsapi.mlb.com/api/v1/teams"
                     "?sportId=1&hydrate=roster(rosterType=40Man)")
        cache_set("_40man", data)
        return data
    except Exception as e:
        print(f"[40man] Error: {e}")
        return {"teams": []}

# ── MLB STATS API ─────────────────────────────────────────────────────────────
def get_today_str():
    et = datetime.now(timezone(timedelta(hours=-4)))  # Eastern Time
    return et.strftime("%Y-%m-%d"), et.hour

def fetch_schedule(date_str):
    cached = cache_get(f"schedule_{date_str}")
    if cached: return cached

    url = (f"https://statsapi.mlb.com/api/v1/schedule"
           f"?sportId=1&date={date_str}"
           f"&hydrate=probablePitcher,lineups,team,venue,weather,linescore,officials")
    try:
        data = fetch(url)
        games = []
        for date_entry in data.get("dates", []):
            for g in date_entry.get("games", []):
                status = g.get("status", {}).get("abstractGameCode", "")
                if status == "F":
                    continue  # skip completed
                away = TEAM_MAP.get(g["teams"]["away"]["team"]["abbreviation"],
                                    g["teams"]["away"]["team"]["abbreviation"])
                home = TEAM_MAP.get(g["teams"]["home"]["team"]["abbreviation"],
                                    g["teams"]["home"]["team"]["abbreviation"])
                ap_raw = g["teams"]["away"].get("probablePitcher", {})
                hp_raw = g["teams"]["home"].get("probablePitcher", {})

                game_time_utc = g.get("gameDate", "")
                try:
                    gt = datetime.strptime(game_time_utc, "%Y-%m-%dT%H:%M:%SZ")
                    gt = gt.replace(tzinfo=timezone.utc).astimezone(
                             timezone(timedelta(hours=-4)))
                    time_str = gt.strftime("%-I:%M %p ET")
                except:
                    time_str = "TBD"

                coords = STADIUM_COORDS.get(home)
                venue_name = coords[2] if coords else g.get("venue", {}).get("name", "")
                pf = PARK_FACTORS.get(home, 100)
                dome = home in DOMES

                # Parse confirmed lineup batting order (player IDs in order)
                lineup_data = g.get("lineups", {})
                home_lineup = [p["id"] for p in lineup_data.get("homePlayers", []) if p.get("id")]
                away_lineup = [p["id"] for p in lineup_data.get("awayPlayers", []) if p.get("id")]

                # Parse HP umpire
                hp_ump = ""
                for o in g.get("officials", []):
                    if o.get("officialType") == "Home Plate":
                        hp_ump = o.get("official", {}).get("fullName", "")
                        break

                games.append({
                    "id": f"{away.lower()}-{home.lower()}-{g['gamePk']}",
                    "gamePk": g["gamePk"],
                    "away": away, "home": home,
                    "time": time_str,
                    "venue": venue_name,
                    "parkFactor": pf,
                    "isDome": dome,
                    "homeLineup": home_lineup,
                    "awayLineup": away_lineup,
                    "hpUmp":     hp_ump,
                    "awayPitcher": {
                        "id": ap_raw.get("id"),
                        "name": ap_raw.get("fullName", "TBD"),
                        "hand": "R"
                    },
                    "homePitcher": {
                        "id": hp_raw.get("id"),
                        "name": hp_raw.get("fullName", "TBD"),
                        "hand": "R"
                    },
                    "status": status
                })

        cache_set(f"schedule_{date_str}", games)
        return games
    except Exception as e:
        print(f"[schedule] Error: {e}")
        return []

def fetch_pitcher_stats(pitcher_id):
    """Fetch ERA, K%, BB%, GB%, FB% from MLB stats API"""
    if not pitcher_id:
        return {}
    cached = cache_get(f"pitcher_{pitcher_id}")
    if cached: return cached

    url = (f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
           f"?stats=season&group=pitching&season=2026")
    try:
        data = fetch(url)
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s = splits[0]["stat"]
        result = {
            "era":    float(s.get("era", 4.50)),
            "whip":   float(s.get("whip", 1.30)),
            "hr9":    float(s.get("homeRunsPer9", 1.10)),
            "k9":     float(s.get("strikeoutsPer9", 7.5)),
            "bb9":    float(s.get("walksPer9", 3.0)),
            "gbPct":  float(s.get("groundOutsToAirouts", 1.0)),
        }
        # Infer quality tier
        era = result["era"]
        result["quality"] = "elite" if era < 2.80 else "danger" if era > 4.80 else "mid"
        result["fbPct"] = 38  # default; Statcast endpoint needed for exact
        result["vel"] = 92.5  # default; Savant needed for exact
        cache_set(f"pitcher_{pitcher_id}", result)
        return result
    except Exception as e:
        print(f"[pitcher {pitcher_id}] Error: {e}")
        return {}

def fetch_injuries():
    """
    Fetch current IL from two sources and merge:
    1. 40-man roster status flags (most accurate — reflects today's IL)
    2. Transactions endpoint (catches recent placements not yet in roster status)
    """
    cached = cache_get("injuries", ttl=ROSTER_TTL)
    if cached: return cached

    injured = set()
    IL_CODES = {"IL10","IL15","IL60","DL","10D","15D","60D",
                "BEREAVEMENT","FAMILY_MEDICAL","SUSPENDED","DES"}

    # ── Primary: 40-man roster status (shared cached fetch) ──────────────────
    try:
        data = fetch_40man_roster()
        for team in data.get("teams", []):
            for player in team.get("roster", []):
                code = (player.get("status", {}).get("code") or "").upper()
                if code in IL_CODES or code.startswith("IL") or code.startswith("DL"):
                    name = player.get("person", {}).get("fullName", "")
                    if name:
                        injured.add(name)
    except Exception as e:
        print(f"[injuries-40man] Error: {e}")

    # ── Secondary: transactions endpoint (recent placements may lag roster) ──
    try:
        tx_url = "https://statsapi.mlb.com/api/v1/transactions?sportId=1&limit=500&transactionType=IL"
        tx = fetch(tx_url)
        for t in tx.get("transactions", []):
            if t.get("typeCode") in ("IL10","IL15","IL60","DL10","DL15","DL60"):
                pname = t.get("player", {}).get("fullName")
                if pname and not t.get("toDate"):  # no activation date = still on IL
                    injured.add(pname)
    except Exception as e:
        print(f"[injuries-tx] Error: {e}")

    print(f"[injuries] {len(injured)} players on IL")
    cache_set("injuries", injured)
    return injured

def fetch_batting_stats_savant():
    """
    Fetch season Statcast batting stats from Baseball Savant CSV endpoint.
    Returns dict keyed by player name.
    """
    cached = cache_get("savant_batting")
    if cached: return cached

    url = (
        "https://baseballsavant.mlb.com/leaderboard/statcast"
        "?type=batter&year=2026&position=&team=&min=10"
        "&csv=true"
    )
    try:
        raw = fetch_savant(url)

        lines = [l for l in raw.strip().split("\n") if l]
        if len(lines) < 2:
            return {}

        headers = lines[0].split(",")
        result = {}
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) < len(headers):
                continue
            row = dict(zip(headers, cols))
            name = row.get("player_name", "").strip()
            if not name:
                continue
            try:
                result[name] = {
                    "barrel_pct":    float(row.get("barrel_batted_rate", 0) or 0),
                    "hard_hit_pct":  float(row.get("hard_hit_percent", 0) or 0),
                    "avg_ev":        float(row.get("avg_hit_speed", 0) or 0),
                    "xwoba":         float(row.get("xwoba", 0) or 0),
                    "xslg":          float(row.get("xslg", 0) or 0),
                    "sweet_spot_pct":float(row.get("sweet_spot_percent", 0) or 0),
                    "launch_angle":  float(row.get("avg_launch_angle", 0) or 0),
                    "pull_pct":      float(row.get("pull_percent", 0) or 0),
                }
            except:
                continue

        cache_set("savant_batting", result)
        print(f"[savant] Loaded {len(result)} batters")
        return result
    except Exception as e:
        print(f"[savant] Error: {e}")
        return {}

def fetch_weather(home_team, game_hour_et):
    """Fetch weather from Open-Meteo for the stadium at game time"""
    coords = STADIUM_COORDS.get(home_team)
    if not coords or home_team in DOMES:
        return {"dome": True, "temp": 72, "wind_mph": 0, "wind_dir": 0,
                "humidity": 50, "pressure": 1013, "note": "Dome — weather excluded"}

    cached = cache_get(f"wx_{home_team}_{game_hour_et}")
    if cached: return cached

    lat, lng, _ = coords
    url = (f"https://api.open-meteo.com/v1/forecast"
           f"?latitude={lat}&longitude={lng}"
           f"&hourly=temperature_2m,relativehumidity_2m,windspeed_10m,"
           f"winddirection_10m,surface_pressure"
           f"&timezone=America%2FNew_York&forecast_days=1")
    try:
        data = fetch(url)
        h = data["hourly"]
        idx = max(0, min(game_hour_et, len(h["temperature_2m"]) - 1))

        temp_c = h["temperature_2m"][idx]
        temp_f = round(temp_c * 9/5 + 32)
        wind_mph = round(h["windspeed_10m"][idx] * 0.621371, 1)
        wind_deg = h["winddirection_10m"][idx]
        humidity = h["relativehumidity_2m"][idx]
        pressure = h["surface_pressure"][idx]

        # Classify wind direction relative to park
        # Most parks: wind from ~180-270° is "out" to CF/RF
        # Simplified: 160-280° = tailwind (out), 0-90 or 315-360 = headwind (in)
        if 160 <= wind_deg <= 280:
            direction = "out"
            note = f"Wind {wind_mph}mph blowing OUT — HR-friendly"
        elif wind_deg <= 80 or wind_deg >= 310:
            direction = "in"
            note = f"Wind {wind_mph}mph blowing IN — suppresses HR"
        else:
            direction = "cross"
            note = f"Crosswind {wind_mph}mph — neutral"

        if wind_mph < 5:
            direction = "calm"
            note = "Calm winds — neutral conditions"

        result = {
            "dome": False,
            "temp": temp_f,
            "wind_mph": wind_mph,
            "wind_dir": wind_deg,
            "direction": direction,
            "humidity": humidity,
            "pressure": round(pressure),
            "note": note
        }
        cache_set(f"wx_{home_team}_{game_hour_et}", result)
        return result
    except Exception as e:
        print(f"[weather {home_team}] Error: {e}")
        return {"dome": False, "temp": 72, "wind_mph": 5, "wind_dir": 180,
                "direction": "cross", "humidity": 55, "pressure": 1013,
                "note": "Weather data unavailable"}

def fetch_active_rosters():
    """
    Fetch all active MLB players from two sources:
    1. sports/1/players — comprehensive, has batSide, but can lag on trades
    2. 40-man roster per team — authoritative for current team post-trade
    Merges both: batSide from source 1, current team overridden by source 2.
    """
    cached = cache_get("rosters", ttl=ROSTER_TTL)
    if cached: return cached

    players = {}

    # ── Source 1: All players with batSide ────────────────────────────────────
    try:
        url = ("https://statsapi.mlb.com/api/v1/sports/1/players"
               "?season=2026&gameType=R")
        data = fetch(url)
        for p in data.get("people", []):
            pid       = p.get("id")
            name      = p.get("fullName", "")
            team_abbr = p.get("currentTeam", {}).get("abbreviation", "")
            team      = TEAM_MAP.get(team_abbr, team_abbr)
            pos       = p.get("primaryPosition", {}).get("abbreviation", "")
            bats      = p.get("batSide", {}).get("code", "R")
            if name:
                players[name] = {"id": pid, "team": team, "pos": pos,
                                 "bats": bats, "name": name}
        print(f"[rosters-primary] {len(players)} players")
    except Exception as e:
        print(f"[rosters-primary] Error: {e}")

    # ── Source 2: 40-man roster — authoritative current-team post-trade ───────
    try:
        data40 = fetch_40man_roster()
        for team_obj in data40.get("teams", []):
            abbr = team_obj.get("abbreviation", "")
            team = TEAM_MAP.get(abbr, abbr)
            for player in team_obj.get("roster", []):
                name = player.get("person", {}).get("fullName", "")
                pid  = player.get("person", {}).get("id")
                pos  = player.get("position", {}).get("abbreviation", "")
                if not name: continue
                if name in players:
                    players[name]["team"] = team  # override with current team
                else:
                    players[name] = {"id": pid, "team": team, "pos": pos,
                                     "bats": "R", "name": name}
        print(f"[rosters-40man] merged → {len(players)} total")
    except Exception as e:
        print(f"[rosters-40man] Error: {e}")

    cache_set("rosters", players)
    return players

def fetch_batting_season_stats():
    """Fetch season batting stats (HR, AVG, OPS, SLG) from MLB Stats API"""
    cached = cache_get("batting_stats")
    if cached: return cached

    url = ("https://statsapi.mlb.com/api/v1/stats"
           "?stats=season&group=hitting&season=2026&sportId=1"
           "&limit=1500&sortStat=gamesPlayed&order=desc")
    try:
        data = fetch(url)
        result = {}
        for split in data.get("stats", [{}])[0].get("splits", []):
            stat = split.get("stat", {})
            player = split.get("player", {})
            name = player.get("fullName", "")
            if not name: continue
            g = int(stat.get("gamesPlayed", 1) or 1)
            hr = int(stat.get("homeRuns", 0) or 0)
            avg = _safe_float(stat.get("avg"))
            slg = _safe_float(stat.get("slg"))
            result[name] = {
                "G": g,
                "HR": hr,
                "AVG": avg,
                "OPS": _safe_float(stat.get("ops")),
                "SLG": slg,
                "OBP": _safe_float(stat.get("obp")),
                "ISO": round(slg - avg, 3),
                "hrPct": round((hr / g) * 100, 1) if g > 0 else 0,
                "paPerG": round(int(stat.get("plateAppearances", 0) or 0) / g, 1) if g > 0 else 3.8,
            }

        cache_set("batting_stats", result)
        print(f"[batting] Loaded {len(result)} batters")
        return result
    except Exception as e:
        print(f"[batting] Error: {e}")
        return {}

def fetch_recent_batting_stats(days=15):
    """Fetch last N days batting stats for hot-streak detection (60/40 blend with season)"""
    cached = cache_get(f"recent_batting_{days}")
    if cached: return cached
    et = datetime.now(timezone(timedelta(hours=-4)))
    end_str   = et.strftime("%Y-%m-%d")
    start_str = (et - timedelta(days=days)).strftime("%Y-%m-%d")
    url = (f"https://statsapi.mlb.com/api/v1/stats"
           f"?stats=byDateRange&group=hitting&season=2026&sportId=1"
           f"&startDate={start_str}&endDate={end_str}&limit=600")
    try:
        data = fetch(url)
        result = {}
        for split in data.get("stats", [{}])[0].get("splits", []):
            stat   = split.get("stat", {})
            name   = split.get("player", {}).get("fullName", "")
            if not name: continue
            g  = int(stat.get("gamesPlayed", 1) or 1)
            hr = int(stat.get("homeRuns", 0) or 0)
            def _f(key):
                v = stat.get(key, "0")
                try: return float(str(v).replace(".","0.",1) if str(v).startswith(".") else v)
                except: return 0.0
            result[name] = {
                "G_recent":      g,
                "HR_recent":     hr,
                "hrPct_recent":  round((hr / g) * 100, 1) if g > 0 else 0,
                "OPS_recent":    _f("ops"),
                "SLG_recent":    _f("slg"),
            }
        cache_set(f"recent_batting_{days}", result)
        print(f"[recent_{days}d] Loaded {len(result)} batters")
        return result
    except Exception as e:
        print(f"[recent_batting] Error: {e}")
        return {}

def fetch_pitcher_game_log(pitcher_id):
    """Last 5 starts: rolling 3-start ERA, days rest, fatigue flag"""
    if not pitcher_id: return {}
    cached = cache_get(f"plog_{pitcher_id}")
    if cached: return cached
    url = (f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
           f"?stats=gameLog&group=pitching&season=2026&limit=5")
    try:
        data   = fetch(url)
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits: return {}
        recent = splits[:3]
        total_er = sum(float(s["stat"].get("earnedRuns", 0) or 0) for s in recent)
        total_ip = sum(float(s["stat"].get("inningsPitched", 0) or 0) for s in recent)
        recent_era = round((total_er / total_ip) * 9, 2) if total_ip > 0 else 4.50
        last_date_str = splits[0].get("date", "")
        days_rest = 5
        if last_date_str:
            try:
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
                days_rest = (datetime.now() - last_date).days
            except: pass
        last_pitches = int(splits[0]["stat"].get("numberOfPitches", 90) or 90)
        result = {
            "recent_era":  recent_era,
            "days_rest":   days_rest,
            "last_pitches":last_pitches,
            "fatigued":    days_rest <= 3 or last_pitches >= 105,
        }
        cache_set(f"plog_{pitcher_id}", result)
        return result
    except Exception as e:
        print(f"[plog {pitcher_id}] Error: {e}")
        return {}

def fetch_home_away_splits():
    """Home/away HR and OPS splits for all batters"""
    cached = cache_get("home_away_splits", ttl=SPLITS_TTL)
    if cached: return cached
    url = ("https://statsapi.mlb.com/api/v1/stats"
           "?stats=homeAndAway&group=hitting&season=2026&sportId=1&limit=700")
    try:
        data = fetch(url)
        result = {}
        for group in data.get("stats", []):
            for split in group.get("splits", []):
                name = split.get("player", {}).get("fullName", "")
                if not name: continue
                is_home = split.get("split", {}).get("code", "").upper() == "H"
                stat = split.get("stat", {})
                g  = int(stat.get("gamesPlayed", 1) or 1)
                hr = int(stat.get("homeRuns", 0) or 0)
                if name not in result: result[name] = {}
                result[name]["home" if is_home else "away"] = {
                    "G": g, "HR": hr,
                    "hrPct": round((hr/g)*100,1) if g>0 else 0,
                    "OPS": _safe_float(stat.get("ops")),
                }
        cache_set("home_away_splits", result)
        print(f"[home_away] Loaded {len(result)} batters")
        return result
    except Exception as e:
        print(f"[home_away] Error: {e}")
        return {}

def fetch_monthly_batting():
    """Current-month batting stats for hot/cold month detection"""
    et    = datetime.now(timezone(timedelta(hours=-4)))
    month = et.month
    cached = cache_get(f"monthly_{month}", ttl=SPLITS_TTL)
    if cached: return cached
    url = ("https://statsapi.mlb.com/api/v1/stats"
           "?stats=byMonth&group=hitting&season=2026&sportId=1&limit=700")
    try:
        data = fetch(url)
        result = {}
        for group in data.get("stats", []):
            for split in group.get("splits", []):
                if str(split.get("split", {}).get("code", "")) != str(month): continue
                name = split.get("player", {}).get("fullName", "")
                if not name: continue
                stat = split.get("stat", {})
                g  = int(stat.get("gamesPlayed", 1) or 1)
                hr = int(stat.get("homeRuns", 0) or 0)
                result[name] = {
                    "G_month":      g,
                    "HR_month":     hr,
                    "hrPct_month":  round((hr/g)*100,1) if g>0 else 0,
                    "OPS_month":    _safe_float(stat.get("ops")),
                }
        cache_set(f"monthly_{month}", result)
        print(f"[monthly] Loaded {len(result)} batters")
        return result
    except Exception as e:
        print(f"[monthly] Error: {e}")
        return {}

def fetch_team_bullpen_era():
    """Team bullpen ERA — opponent bullpen quality affects late-game HR risk"""
    cached = cache_get("bullpen_era")
    if cached: return cached
    url = ("https://statsapi.mlb.com/api/v1/stats"
           "?stats=season&group=pitching&season=2026&sportId=1"
           "&position=RP&limit=600")
    try:
        data = fetch(url)
        team_er, team_ip = {}, {}
        for split in data.get("stats", [{}])[0].get("splits", []):
            abbr = split.get("team", {}).get("abbreviation", "")
            team = TEAM_MAP.get(abbr, abbr)
            stat = split.get("stat", {})
            er   = float(stat.get("earnedRuns", 0) or 0)
            ip   = float(stat.get("inningsPitched", 0) or 0)
            team_er[team] = team_er.get(team, 0) + er
            team_ip[team] = team_ip.get(team, 0) + ip
        result = {t: round((team_er[t]/team_ip[t])*9,2) for t in team_er if team_ip.get(t,0)>0}
        cache_set("bullpen_era", result)
        print(f"[bullpen] Loaded {len(result)} teams")
        return result
    except Exception as e:
        print(f"[bullpen] Error: {e}")
        return {}

def fetch_pitcher_savant_allowed():
    """Pitcher Statcast allowed stats: barrel%, hard hit% against — better vuln signal"""
    cached = cache_get("pitcher_savant_allowed", ttl=SPLITS_TTL)
    if cached: return cached
    url = (
        "https://baseballsavant.mlb.com/leaderboard/statcast"
        "?type=pitcher&year=2026&position=&team=&min=25&csv=true"
    )
    try:
        raw  = fetch_savant(url)
        lines = [l for l in raw.strip().split("\n") if l]
        if len(lines) < 2: return {}
        hdrs = lines[0].split(",")
        result  = {}
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) < len(hdrs): continue
            row  = dict(zip(hdrs, cols))
            name = row.get("player_name", "").strip()
            if not name: continue
            try:
                result[name] = {
                    "barrel_allowed":   float(row.get("barrel_batted_rate", 0) or 0),
                    "hard_hit_allowed": float(row.get("hard_hit_percent", 0) or 0),
                    "xwoba_against":    float(row.get("xwoba", 0) or 0),
                    "xslg_against":     float(row.get("xslg", 0) or 0),
                }
            except: continue
        cache_set("pitcher_savant_allowed", result)
        print(f"[pitcher_sav] Loaded {len(result)} pitchers")
        return result
    except Exception as e:
        print(f"[pitcher_sav] Error: {e}")
        return {}

def hr_probability(score):
    """Map 0–99 composite score to estimated HR probability % per game."""
    prob = 1.5 + (score / 99) ** 1.75 * 29.5
    return round(max(1.0, min(32.0, prob)), 1)

def ump_zone_adj(ump_name, live_ump_scores=None):
    """Zone adjustment: live UmpScorecards score first, fallback to hardcoded dict."""
    if live_ump_scores and ump_name in live_ump_scores:
        return float(live_ump_scores[ump_name])
    return UMP_ZONES.get(ump_name, 0.0)

# ── LIVE FREE API INTEGRATIONS ────────────────────────────────────────────────

def fetch_ump_zone_live(date_str):
    """Live umpire zone scores from UmpScorecards.com"""
    cached = cache_get(f"ump_live_{date_str}")
    if cached: return cached
    result = {}
    for url in [
        f"https://umpscorecards.com/api/games/?date={date_str}",
        "https://umpscorecards.com/api/umpires/",
    ]:
        try:
            data = fetch(url)
            items = data if isinstance(data, list) else \
                    data.get("games", data.get("umpires", data.get("data", [])))
            for item in items:
                name  = (item.get("hp_umpire") or item.get("umpire") or
                         item.get("homeplate_umpire") or item.get("name") or "")
                favor = (item.get("favor") or item.get("zone_favor") or
                         item.get("run_impact") or item.get("favor_score") or 0)
                if name:
                    result[name] = float(favor)
            if result: break
        except Exception as e:
            print(f"[umpscorecards] {url}: {e}")
    if result:
        cache_set(f"ump_live_{date_str}", result)
        print(f"[umpscorecards] {len(result)} ump scores")
    return result

def fetch_prizepicks_mlb():
    """PrizePicks MLB HR prop lines — displayed on player cards."""
    cached = cache_get("prizepicks", ttl=1800)
    if cached: return cached
    url = ("https://api.prizepicks.com/projections"
           "?league_id=2&per_page=250&single_stat=true&game_mode=pickem")
    try:
        req = Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept":     "application/json",
            "Referer":    "https://app.prizepicks.com/"
        })
        with urlopen(req, timeout=API_TIMEOUT) as r:
            data = json.loads(r.read().decode())
        result = {}
        for proj in data.get("data", []):
            attrs     = proj.get("attributes", {})
            stat_type = attrs.get("stat_type", "")
            if stat_type in ("Home Runs", "HR", "Home Runs (SGP)"):
                name = (attrs.get("name") or attrs.get("player_name") or "").strip()
                line = attrs.get("line_score")
                if name and line is not None:
                    result[name] = {"line": float(line), "type": attrs.get("odds_type", "")}
        cache_set("prizepicks", result)
        print(f"[prizepicks] {len(result)} HR props")
        return result
    except Exception as e:
        print(f"[prizepicks] Error: {e}")
        return {}

def fetch_sprint_speed():
    """Sprint speed from Baseball Savant — displayed in player cards."""
    cached = cache_get("sprint_speed", ttl=SPLITS_TTL)
    if cached: return cached
    url = ("https://baseballsavant.mlb.com/leaderboard/sprint_speed"
           "?min_competitive=50&player_type=batter&year=2026&csv=true")
    try:
        raw  = fetch_savant(url)
        lines = [l for l in raw.strip().split("\n") if l]
        if len(lines) < 2: return {}
        hdrs = lines[0].split(",")
        result  = {}
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) < len(hdrs): continue
            row  = dict(zip(hdrs, cols))
            name = row.get("player_name", "").strip()
            if not name: continue
            try:
                result[name] = round(float(row.get("sprint_speed", 0) or 0), 1)
            except: continue
        cache_set("sprint_speed", result)
        print(f"[sprint_speed] {len(result)} players")
        return result
    except Exception as e:
        print(f"[sprint_speed] Error: {e}")
        return {}

def fetch_game_totals(date_str):
    """O/U game totals from The Odds API — run environment signal.
    Requires ODDS_API_KEY env var (free at the-odds-api.com, 500 req/mo)."""
    if not ODDS_API_KEY: return {}
    cached = cache_get(f"totals_{date_str}")
    if cached: return cached
    url = (f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/"
           f"?apiKey={ODDS_API_KEY}&regions=us&markets=totals&dateFormat=iso")
    try:
        data = fetch(url)
        result = {}
        for game in data:
            away = game.get("away_team", "").lower()
            home = game.get("home_team", "").lower()
            away_abbr = next((v for k, v in ODDS_TEAM_MAP.items() if k in away), None)
            home_abbr = next((v for k, v in ODDS_TEAM_MAP.items() if k in home), None)
            if not (away_abbr and home_abbr): continue
            for bm in game.get("bookmakers", [])[:2]:
                for market in bm.get("markets", []):
                    if market.get("key") == "totals":
                        for outcome in market.get("outcomes", []):
                            if outcome.get("name") == "Over":
                                result[f"{away_abbr}_{home_abbr}"] = float(outcome.get("point", 8.5))
        cache_set(f"totals_{date_str}", result)
        print(f"[odds] {len(result)} game totals")
        return result
    except Exception as e:
        print(f"[odds] Error: {e}")
        return {}

# ── SCORING ENGINE ────────────────────────────────────────────────────────────
def wind_adj(wx):
    if wx.get("dome"): return 0
    mph = wx.get("wind_mph", 0)
    d = wx.get("direction", "calm")
    if d == "out": return round(mph * 0.55)
    if d == "in":  return -round(mph * 0.65)
    return 0

def temp_adj(t):
    if t >= 85: return 3
    if t >= 75: return 1
    if t <= 55: return -2
    if t <= 65: return -1
    return 0

def baro_adj(p):
    if p < 990: return 3
    if p < 1005: return 1
    if p > 1020: return -1
    return 0

def humid_adj(rh):
    if rh >= 70: return -1
    if rh <= 40: return 1
    return 0

def platoon_adj(bats, pitcher_hand):
    if bats == "S": return 4
    return 8 if bats != pitcher_hand else -4

def pitcher_vuln(p_stats):
    era = p_stats.get("era", 4.50)
    fb  = p_stats.get("fbPct", 38)
    vel = p_stats.get("vel", 92.5)
    q   = p_stats.get("quality", "mid")
    s = 28 + (era - 3.5) * 5.5
    s += (fb - 38) * 0.35
    if vel < 91: s += 3
    if q == "danger": s += 16
    if q == "elite":  s -= 12
    return max(0, min(100, s))

def pull_adj(bats, home_team):
    pb   = PULL_BONUS.get(home_team, {})
    side = "L" if bats in ("L", "S") else "R"
    return pb.get(side, 0)

def lineup_adj(pos):
    return LINEUP_BONUS.get(pos, 0)

def game_total_adj(total):
    """Run environment from O/U total: high-total games = more HRs expected."""
    if not total: return 0
    if total >= 10.0: return 3
    if total >=  9.0: return 2
    if total >=  8.0: return 1
    if total <=  6.5: return -2
    if total <=  7.5: return -1
    return 0

# ── ROSTER & MATCHUP DATA FROM MLB.COM ───────────────────────────────────────

def fetch_pitcher_details_batch(pitcher_ids):
    """Batch-fetch pitcher throwing hand from MLB people API."""
    if not pitcher_ids: return {}
    sorted_ids = sorted(int(i) for i in pitcher_ids if i)
    if not sorted_ids: return {}
    cache_key = f"pdet_{'_'.join(str(i) for i in sorted_ids)}"
    cached = cache_get(cache_key, ttl=SPLITS_TTL)
    if cached: return cached
    ids_str = ",".join(str(i) for i in sorted_ids)
    url = f"https://statsapi.mlb.com/api/v1/people?personIds={ids_str}"
    try:
        data = fetch(url)
        result = {}
        for person in data.get("people", []):
            pid = person.get("id")
            if pid:
                result[pid] = {
                    "hand":     person.get("pitchHand", {}).get("code", "R"),
                    "fullName": person.get("fullName", ""),
                }
        cache_set(cache_key, result)
        print(f"[pitcher_details] {len(result)} pitchers fetched")
        return result
    except Exception as e:
        print(f"[pitcher_details] Error: {e}")
        return {}


def fetch_h2h_vs_pitcher(pitcher_id):
    """Career batter-vs-pitcher matchup stats for all batters who've faced this pitcher."""
    if not pitcher_id: return {}
    cached = cache_get(f"h2h_{pitcher_id}", ttl=SPLITS_TTL)
    if cached: return cached
    url = (f"https://statsapi.mlb.com/api/v1/stats"
           f"?stats=vsPlayer&group=hitting"
           f"&opposingPlayerId={pitcher_id}&limit=2000")
    try:
        data = fetch(url)
        result = {}
        for group in data.get("stats", []):
            for split in group.get("splits", []):
                name = split.get("player", {}).get("fullName", "")
                stat = split.get("stat", {})
                if not name: continue
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 3: continue
                result[name] = {
                    "ab":  ab,
                    "h":   int(stat.get("hits", 0) or 0),
                    "hr":  int(stat.get("homeRuns", 0) or 0),
                    "rbi": int(stat.get("rbi", 0) or 0),
                    "avg": _safe_float(stat.get("avg")),
                    "ops": _safe_float(stat.get("ops")),
                    "slg": _safe_float(stat.get("slg")),
                }
        cache_set(f"h2h_{pitcher_id}", result)
        print(f"[h2h] Pitcher {pitcher_id}: {len(result)} batters with history")
        return result
    except Exception as e:
        print(f"[h2h {pitcher_id}] Error: {e}")
        return {}


def fetch_active_roster_single(team_abbr):
    """Fetch active 26-man roster for one team — who's actually available today."""
    cached = cache_get(f"active_roster_{team_abbr}", ttl=ROSTER_TTL)
    if cached: return cached
    team_id = MLB_TEAM_IDS.get(team_abbr)
    if not team_id: return set()
    url = (f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
           f"?rosterType=Active&season=2026")
    try:
        data = fetch(url)
        names = set()
        for p in data.get("roster", []):
            name = p.get("person", {}).get("fullName", "")
            if name: names.add(name)
        cache_set(f"active_roster_{team_abbr}", names)
        print(f"[active_roster] {team_abbr}: {len(names)} active players")
        return names
    except Exception as e:
        print(f"[active_roster {team_abbr}] {e}")
        return set()


def h2h_adj(h2h):
    """Score adjustment based on career H2H matchup vs this specific pitcher."""
    if not h2h or h2h.get("ab", 0) < 3: return 0
    ab  = h2h["ab"]
    hr  = h2h.get("hr", 0)
    ops = h2h.get("ops", 0.0)
    adj = 0
    if hr >= 3:              adj += 6
    elif hr == 2:            adj += 4
    elif hr == 1:            adj += 2
    if ops >= 0.900 and ab >= 5:  adj += 2
    elif ops <= 0.450 and ab >= 7: adj -= 2
    return adj


def calc_composite(batter_stats, savant_stats, pitcher_stats, pf, wx,
                   recent_stats=None, pitcher_log=None, lineup_pos=0, home_team="",
                   home_away_splits=None, is_home=False,
                   monthly_stats=None, bullpen_era=4.50,
                   pitcher_sav=None, ump_score=0.0, game_total=0.0,
                   h2h=None):

    hr_pct   = batter_stats.get("hrPct", 0)
    ops      = batter_stats.get("OPS", 0)
    slg      = batter_stats.get("SLG", 0)
    iso      = batter_stats.get("ISO", 0)
    pa_per_g = batter_stats.get("paPerG", 3.8)
    barrel   = savant_stats.get("barrel_pct", 8)
    hard_hit = savant_stats.get("hard_hit_pct", 40)
    sweet    = savant_stats.get("sweet_spot_pct", 36)
    pull_pct = savant_stats.get("pull_pct", 40)
    bats     = batter_stats.get("bats", "R")
    ph       = pitcher_stats.get("hand", "R")

    # ── 1. Blend recent 15-day form (60/40 with season) ───────────────────────
    if recent_stats and recent_stats.get("G_recent", 0) >= 7:
        hr_pct = recent_stats["hrPct_recent"] * 0.60 + hr_pct * 0.40
        ops    = recent_stats["OPS_recent"]   * 0.55 + ops    * 0.45
        slg    = recent_stats["SLG_recent"]   * 0.55 + slg    * 0.45

    # ── 2. Blend home/away split (35% weight, 15+ game minimum) ──────────────
    if home_away_splits:
        key   = "home" if is_home else "away"
        split = home_away_splits.get(key, {})
        if split.get("G", 0) >= 15 and split.get("hrPct", 0) > 0:
            hr_pct = split["hrPct"] * 0.35 + hr_pct * 0.65
            ops    = split["OPS"]   * 0.30 + ops    * 0.70

    # ── 3. Blend current-month split (25% weight, 10+ game minimum) ──────────
    if monthly_stats and monthly_stats.get("G_month", 0) >= 10:
        m_hr = monthly_stats["hrPct_month"]
        if m_hr > 0:
            hr_pct = m_hr * 0.25 + hr_pct * 0.75

    # ── PROFILE: inherent power (reduced weights so stars don't auto-dominate) ──
    profile = 0
    profile += min(hr_pct * 1.8, 20)           # was 2.3→27; now cap at 20
    profile += min((ops - 0.600) * 28, 14)     # was 36→20; now cap at 14
    profile += min((slg - 0.350) * 22, 10)     # was 30→15; now cap at 10
    profile += min((barrel - 8) * 0.7, 8)
    profile += min((hard_hit - 40) * 0.22, 5)
    profile += min((iso - 0.180) * 18, 4)
    profile += min((sweet - 36) * 0.2, 2)
    if pa_per_g >= 4.5: profile += 3
    elif pa_per_g >= 4.2: profile += 2
    elif pa_per_g < 3.0: profile -= 2
    if pull_pct > 40:
        profile += pull_adj(bats, home_team) * ((pull_pct - 38) / 22)

    # ── SITUATION: today's opportunity (increased weights — this is where VALUE hides)
    era = pitcher_stats.get("era", 4.50)
    if pitcher_log and pitcher_log.get("recent_era"):
        era = pitcher_log["recent_era"] * 0.60 + era * 0.40
    fb  = pitcher_stats.get("fbPct", 38)
    vel = pitcher_stats.get("vel", 92.5)
    q   = pitcher_stats.get("quality", "mid")
    pv  = 28 + (era - 3.5) * 5.5
    pv += (fb - 38) * 0.35
    if vel < 91: pv += 3
    if q == "danger": pv += 16
    if q == "elite":  pv -= 12
    if pitcher_log and pitcher_log.get("fatigued"):          pv += 7
    if pitcher_log and pitcher_log.get("days_rest", 5) <= 3: pv += 5
    if pitcher_sav:
        pv += (pitcher_sav.get("barrel_allowed", 8) - 8) * 0.4
        pv += (pitcher_sav.get("hard_hit_allowed", 38) - 38) * 0.10

    situ = 0
    situ += max(0, min(100, pv)) * 0.25        # pitcher vuln: was 0.17, now 0.25
    situ += ((pf - 100) / 50) * 12             # park: was 8, now 12
    situ += wind_adj(wx) * 0.70                # wind: was 0.48, now 0.70
    situ += temp_adj(wx.get("temp", 72))
    situ += baro_adj(wx.get("pressure", 1013))
    situ += humid_adj(wx.get("humidity", 55))
    situ += platoon_adj(bats, ph)
    situ += lineup_adj(lineup_pos)
    situ += ump_score * 2.5                    # ump: was 2.0
    situ += game_total_adj(game_total)
    if bullpen_era >= 5.20:   situ += 4
    elif bullpen_era >= 4.80: situ += 2
    elif bullpen_era >= 4.50: situ += 1
    elif bullpen_era < 3.50:  situ -= 2

    return max(0, min(99, round(profile + situ + h2h_adj(h2h))))


def calc_situ_score(pitcher_stats, pf, wx, pitcher_log=None, pitcher_sav=None,
                    bullpen_era=4.50, ump_score=0.0, game_total=0.0,
                    bats="R", lineup_pos=0):
    """Situational score only — how good is TODAY'S opportunity regardless of player.
    High situ = value spot. A lower-profile player with high situ is a VALUE pick."""
    era = pitcher_stats.get("era", 4.50)
    if pitcher_log and pitcher_log.get("recent_era"):
        era = pitcher_log["recent_era"] * 0.60 + era * 0.40
    fb  = pitcher_stats.get("fbPct", 38)
    vel = pitcher_stats.get("vel", 92.5)
    q   = pitcher_stats.get("quality", "mid")
    ph  = pitcher_stats.get("hand", "R")
    pv  = 28 + (era - 3.5) * 5.5 + (fb - 38) * 0.35
    if vel < 91:  pv += 3
    if q == "danger": pv += 16
    if q == "elite":  pv -= 12
    if pitcher_log and pitcher_log.get("fatigued"):          pv += 7
    if pitcher_log and pitcher_log.get("days_rest", 5) <= 3: pv += 5
    if pitcher_sav:
        pv += (pitcher_sav.get("barrel_allowed", 8) - 8) * 0.4
        pv += (pitcher_sav.get("hard_hit_allowed", 38) - 38) * 0.10
    s = max(0, min(100, pv)) * 0.25
    s += ((pf - 100) / 50) * 12
    s += wind_adj(wx) * 0.70
    s += temp_adj(wx.get("temp", 72))
    s += baro_adj(wx.get("pressure", 1013))
    s += humid_adj(wx.get("humidity", 55))
    s += platoon_adj(bats, ph)
    s += lineup_adj(lineup_pos)
    s += ump_score * 2.5
    s += game_total_adj(game_total)
    if bullpen_era >= 5.20:   s += 4
    elif bullpen_era >= 4.80: s += 2
    elif bullpen_era >= 4.50: s += 1
    elif bullpen_era < 3.50:  s -= 2
    return round(max(0, min(60, s)), 1)

def get_tier(score):
    if score >= 80: return "S"
    if score >= 65: return "A"
    if score >= 50: return "B"
    if score >= 35: return "C"
    return "D"

# ── MAIN DATA ASSEMBLY ────────────────────────────────────────────────────────
def build_daily_data(date_str):
    cached = cache_get(f"daily_{date_str}")
    if cached: return cached

    print(f"[build] Fetching fresh data for {date_str}...")

    # ── Step 1: Schedule first (need pitcher IDs + game list for step 2) ─────
    games = fetch_schedule(date_str)

    # Collect unique pitcher IDs, game hours, and teams
    pitcher_ids = set()
    game_wx_keys = {}  # game_id → (home, hour)
    all_game_teams = set()
    for g in games:
        for side in ["awayPitcher", "homePitcher"]:
            pid = g[side].get("id")
            if pid: pitcher_ids.add(pid)
        home = g["home"]
        try:
            t = g["time"].replace(" ET","").replace(" PM","").replace(" AM","")
            hour = int(t.split(":")[0])
            is_pm = "PM" in g["time"]
            if is_pm and hour != 12: hour += 12
            if not is_pm and hour == 12: hour = 0
        except:
            hour = 19
        game_wx_keys[g["id"]] = (home, hour)
        all_game_teams.add(g["away"])
        all_game_teams.add(g["home"])

    # ── Step 2: Fetch EVERYTHING in one parallel batch ────────────────────────
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as ex:
        # Core data
        f_batting  = ex.submit(fetch_batting_season_stats)
        f_recent   = ex.submit(fetch_recent_batting_stats, 15)
        f_savant   = ex.submit(fetch_batting_stats_savant)
        f_40man    = ex.submit(fetch_40man_roster)
        f_rosters  = ex.submit(fetch_active_rosters)
        f_injuries = ex.submit(fetch_injuries)
        # Enrichment (longer TTL, fail gracefully)
        f_homeaway = ex.submit(fetch_home_away_splits)
        f_monthly  = ex.submit(fetch_monthly_batting)
        f_bullpen  = ex.submit(fetch_team_bullpen_era)
        f_psav     = ex.submit(fetch_pitcher_savant_allowed)
        # New free integrations
        f_ump_live = ex.submit(fetch_ump_zone_live, date_str)
        f_pp       = ex.submit(fetch_prizepicks_mlb)
        f_sprint   = ex.submit(fetch_sprint_speed)
        f_totals   = ex.submit(fetch_game_totals, date_str)
        # All pitcher stats + logs in parallel
        f_p_stats  = {pid: ex.submit(fetch_pitcher_stats,    pid) for pid in pitcher_ids}
        f_p_logs   = {pid: ex.submit(fetch_pitcher_game_log, pid) for pid in pitcher_ids}
        # All weather in parallel
        f_wx = {}
        for gid, (home, hour) in game_wx_keys.items():
            f_wx[gid] = ex.submit(fetch_weather, home, hour)
        # Pitcher throwing hand (batch), active 26-man rosters, H2H matchups
        f_pitcher_details = ex.submit(fetch_pitcher_details_batch, list(pitcher_ids))
        f_active = {team: ex.submit(fetch_active_roster_single, team) for team in all_game_teams}
        f_h2h    = {pid: ex.submit(fetch_h2h_vs_pitcher, pid) for pid in pitcher_ids}

    # Collect results safely
    def safe(f):
        try: return f.result() or {}
        except: return {}

    batting         = safe(f_batting)
    recent          = safe(f_recent)
    savant          = safe(f_savant)
    rosters         = safe(f_rosters)
    injuries        = safe(f_injuries)
    home_away       = safe(f_homeaway)
    monthly         = safe(f_monthly)
    bullpen         = safe(f_bullpen)
    pitcher_sav_map = safe(f_psav)
    ump_live        = safe(f_ump_live)
    prizepicks      = safe(f_pp)
    sprint_speed    = safe(f_sprint)
    game_totals     = safe(f_totals)
    pitcher_stats   = {pid: safe(f) for pid, f in f_p_stats.items()}
    pitcher_logs    = {pid: safe(f) for pid, f in f_p_logs.items()}
    weather_map     = {gid: safe(f) for gid, f in f_wx.items()}
    pitcher_details = safe(f_pitcher_details)
    active_rosters  = {}
    for team, f in f_active.items():
        try: active_rosters[team] = f.result() or set()
        except: active_rosters[team] = set()
    h2h_maps = {pid: (safe(f) or {}) for pid, f in f_h2h.items()}
    _, et_hour = get_today_str()

    # ── Step 3: Assign pre-fetched data to games (zero network calls) ─────────
    for g in games:
        g["weather"] = weather_map.get(g["id"], {
            "dome": False, "temp": 72, "wind_mph": 5, "wind_dir": 180,
            "direction": "cross", "humidity": 55, "pressure": 1013, "note": ""})

        ump_score  = ump_zone_adj(g.get("hpUmp", ""), ump_live)
        total_key  = f"{g['away']}_{g['home']}"
        game_total = game_totals.get(total_key, 0.0)
        for side in ["awayPitcher", "homePitcher"]:
            pid   = g[side].get("id")
            name  = g[side].get("name", "")
            stats = pitcher_stats.get(pid, {})
            plog  = pitcher_logs.get(pid, {})
            psav  = pitcher_sav_map.get(name, {})
            g[side].update(stats)
            g[side]["_log"]  = plog
            g[side]["_psav"] = psav
            if not stats:
                g[side].update({"era": 4.50, "quality": "mid", "fbPct": 38, "vel": 92.5})
            # Apply correct throwing hand from MLB people API
            if pid:
                det = pitcher_details.get(pid, {})
                if det.get("hand"):
                    g[side]["hand"] = det["hand"]

        pf = g["parkFactor"]
        wx = g["weather"]
        home_lineup = g.get("homeLineup", [])
        away_lineup = g.get("awayLineup", [])
        opp_bullpen_away = bullpen.get(g["away"], 4.50)
        opp_bullpen_home = bullpen.get(g["home"], 4.50)

        # Build player projections for this game
        home = g["home"]   # fix: was leaking from earlier loop
        players = []
        game_teams = {g["away"], g["home"]}

        for name, roster_info in rosters.items():
            if name in injuries: continue
            team = roster_info.get("team", "")
            if team not in game_teams: continue
            pos = roster_info.get("pos", "")
            if pos in ("P", "SP", "RP"): continue

            # Only include players on today's active 26-man roster (when data available)
            team_active = active_rosters.get(team, set())
            if team_active and name not in team_active:
                continue

            bat_stat = dict(batting.get(name, {}))
            if bat_stat.get("G", 0) < 1: continue   # was < 5; now include anyone with ≥1 game

            sav          = savant.get(name, {})
            recent_stat  = recent.get(name, {})
            ha_splits    = home_away.get(name, {})
            month_stat   = monthly.get(name, {})
            is_away      = team == g["away"]
            is_home_game = not is_away
            opp_pitcher  = g["homePitcher"] if is_away else g["awayPitcher"]
            pitcher_log  = opp_pitcher.get("_log", {})
            pitcher_sav  = opp_pitcher.get("_psav", {})
            opp_bullpen  = opp_bullpen_home if is_away else opp_bullpen_away
            pid          = roster_info.get("id")
            lineup       = away_lineup if is_away else home_lineup
            lineup_pos   = (lineup.index(pid) + 1) if pid and pid in lineup else 0

            bat_stat["bats"] = roster_info.get("bats", "R")
            pp_data = prizepicks.get(name, {})
            # Career batter vs pitcher H2H matchup stats
            opp_pid = opp_pitcher.get("id")
            h2h = h2h_maps.get(opp_pid, {}).get(name, {}) if opp_pid else {}
            score = calc_composite(
                bat_stat, sav, opp_pitcher, pf, wx,
                recent_stats=recent_stat,
                pitcher_log=pitcher_log,
                lineup_pos=lineup_pos,
                home_team=home,
                home_away_splits=ha_splits,
                is_home=is_home_game,
                monthly_stats=month_stat,
                bullpen_era=opp_bullpen,
                pitcher_sav=pitcher_sav,
                ump_score=ump_score,
                game_total=game_total,
                h2h=h2h,
            )
            tier = get_tier(score)

            recent_hr_pct = recent_stat.get("hrPct_recent", 0)
            hot_streak    = (recent_hr_pct > bat_stat.get("hrPct", 0) * 1.35
                             and recent_stat.get("G_recent", 0) >= 7)
            month_hr_pct  = month_stat.get("hrPct_month", 0)
            hot_month     = (month_hr_pct > bat_stat.get("hrPct", 0) * 1.25
                             and month_stat.get("G_month", 0) >= 10)

            players.append({
                "name":             name,
                "mlbId":            roster_info.get("id"),
                "team":             team,
                "pos":              pos,
                "bats":             roster_info.get("bats", "R"),
                "G":                bat_stat.get("G", 0),
                "HR":               bat_stat.get("HR", 0),
                "hrPct":            bat_stat.get("hrPct", 0),
                "AVG":              bat_stat.get("AVG", 0),
                "OPS":              bat_stat.get("OPS", 0),
                "SLG":              bat_stat.get("SLG", 0),
                "ISO":              bat_stat.get("ISO", 0),
                "barrel":           sav.get("barrel_pct", 0),
                "hardHit":          sav.get("hard_hit_pct", 0),
                "avgEV":            sav.get("avg_ev", 0),
                "xwOBA":            sav.get("xwoba", 0),
                "sweetSpot":        sav.get("sweet_spot_pct", 0),
                "pullPct":          sav.get("pull_pct", 0),
                "pitcher":          opp_pitcher.get("name", "TBD"),
                "pitcherEra":       opp_pitcher.get("era", 4.50),
                "pitcherRecentEra": round(pitcher_log.get("recent_era", opp_pitcher.get("era", 4.50)), 2),
                "pitcherHand":      opp_pitcher.get("hand", "R"),
                "pitcherFatigued":  pitcher_log.get("fatigued", False),
                "pitcherBarrelAllowed": round(pitcher_sav.get("barrel_allowed", 0), 1),
                "lineupPos":        lineup_pos,
                "confirmed":        lineup_pos > 0,
                "isHome":           is_home_game,
                "recentHR":         recent_stat.get("HR_recent", 0),
                "recentHRPct":      recent_hr_pct,
                "monthHRPct":       month_hr_pct,
                "hotStreak":        hot_streak,
                "hotMonth":         hot_month,
                "oppBullpenEra":    opp_bullpen,
                "hpUmp":            g.get("hpUmp", ""),
                "umpScore":         ump_score,
                "gameTotal":        game_total,
                "ppLine":           pp_data.get("line", None),
                "ppType":           pp_data.get("type", ""),
                "sprintSpeed":      sprint_speed.get(name, None),
                "hrProb":           hr_probability(score),
                "h2h":              h2h if h2h.get("ab", 0) >= 3 else None,
                "gameId":           g["id"],
                "score":            score,
                "tier":             tier,
            })

        # ── Diagnostic: print filter counts when a game has 0 players ──────────
        if not players:
            n_teams  = sum(1 for _,r in rosters.items() if r.get("team","") in game_teams)
            n_pos    = sum(1 for _,r in rosters.items() if r.get("team","") in game_teams and r.get("pos","") not in ("P","SP","RP"))
            n_bat    = sum(1 for n,r in rosters.items() if r.get("team","") in game_teams and r.get("pos","") not in ("P","SP","RP") and batting.get(n,{}).get("G",0) >= 1)
            sample   = [(n, r.get("team"), r.get("pos"), batting.get(n,{}).get("G",0)) for n,r in list(rosters.items()) if r.get("team","") in game_teams][:4]
            print(f"[debug-0pl] {g['away']}@{g['home']} teams={game_teams}")
            print(f"[debug-0pl]  in_teams={n_teams} not_pitcher={n_pos} has_batting_G>=1={n_bat}")
            print(f"[debug-0pl]  sample={sample}")

        players.sort(key=lambda x: x["score"], reverse=True)
        g["players"]    = players
        g["topPick"]    = players[0] if players else None
        g["gameTotal"]  = game_total
        g["hpUmpScore"] = ump_score

    # Top 5 across all games
    all_players = []
    for g in games:
        all_players.extend(g.get("players", []))
    all_players.sort(key=lambda x: x["score"], reverse=True)
    top5 = all_players[:5]

    result = {
        "date":      date_str,
        "generated": datetime.now(timezone.utc).isoformat(),
        "games":     games,
        "top5":      top5,
        "injuryList": list(injuries),
        "cacheExpires": int(time.time()) + CACHE_TTL,
    }

    cache_set(f"daily_{date_str}", result)
    print(f"[build] Done — {len(games)} games, {len(all_players)} players")
    return result

# ── HTTP SERVER ───────────────────────────────────────────────────────────────
HTML_FILE = "hr-prop-board.html"

class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path, content_type):
        try:
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", len(body))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"File not found")

    def do_GET(self):
        path = urlparse(self.path).path
        params = parse_qs(urlparse(self.path).query)

        if path == "/" or path == "/index.html":
            self.send_file(HTML_FILE, "text/html; charset=utf-8")

        elif path == "/api/data":
            date_str = params.get("date", [get_today_str()[0]])[0]
            try:
                data = build_daily_data(date_str)
                self.send_json(data)
            except Exception as e:
                traceback.print_exc()
                self.send_json({"error": str(e)}, 500)

        elif path == "/api/status":
            date_str, _ = get_today_str()
            key = f"daily_{date_str}"
            with _cache_lock:
                entry = _cache.get(key)
            self.send_json({
                "cached":   entry is not None,
                "cacheAge": round(time.time() - entry["ts"]) if entry else None,
                "ttl":      CACHE_TTL,
                "date":     date_str,
            })

        elif path == "/api/search":
            q = params.get("q", [""])[0].strip()
            if len(q) < 2:
                self.send_json([])
                return
            try:
                rosters      = fetch_active_rosters()
                batting      = fetch_batting_season_stats()
                savant       = fetch_batting_stats_savant()
                injuries     = fetch_injuries()
                sprint_map   = fetch_sprint_speed()
                pp_map       = fetch_prizepicks_mlb()

                q_lower = q.lower()
                # Substring matches first
                sub_matches = [n for n in rosters if q_lower in n.lower()]
                # Fuzzy matches
                fuzzy = difflib.get_close_matches(
                    q_lower, [n.lower() for n in rosters], n=10, cutoff=0.45)
                for fm in fuzzy:
                    orig = next((n for n in rosters if n.lower() == fm), None)
                    if orig and orig not in sub_matches:
                        sub_matches.append(orig)

                results = []
                for name in sub_matches[:25]:
                    roster_info = rosters[name]
                    if roster_info.get("pos") in ("P", "SP", "RP"):
                        continue
                    team     = roster_info.get("team", "")
                    bat_stat = dict(batting.get(name, {}))
                    sav      = savant.get(name, {})
                    bat_stat["bats"] = roster_info.get("bats", "R")
                    home_pf  = PARK_FACTORS.get(team, 100)
                    neutral_pitcher = {"era": 4.50, "quality": "mid", "fbPct": 38,
                                       "vel": 92.5, "hand": "R"}
                    neutral_wx = {"dome": False, "temp": 72, "wind_mph": 0,
                                  "wind_dir": 180, "direction": "calm",
                                  "humidity": 55, "pressure": 1013}
                    score = calc_composite(bat_stat, sav, neutral_pitcher, home_pf, neutral_wx)
                    results.append({
                        "name":       name,
                        "team":       team,
                        "pos":        roster_info.get("pos", ""),
                        "bats":       roster_info.get("bats", "R"),
                        "G":          bat_stat.get("G", 0),
                        "HR":         bat_stat.get("HR", 0),
                        "AVG":        round(bat_stat.get("AVG", 0), 3),
                        "OPS":        round(bat_stat.get("OPS", 0), 3),
                        "SLG":        round(bat_stat.get("SLG", 0), 3),
                        "ISO":        round(bat_stat.get("ISO", 0), 3),
                        "hrPct":      round(bat_stat.get("hrPct", 0), 1),
                        "barrel":     round(sav.get("barrel_pct", 0), 1),
                        "hardHit":    round(sav.get("hard_hit_pct", 0), 1),
                        "avgEV":      round(sav.get("avg_ev", 0), 1),
                        "xwOBA":      round(sav.get("xwoba", 0), 3),
                        "sweetSpot":  round(sav.get("sweet_spot_pct", 0), 1),
                        "onIL":        name in injuries,
                        "score":       score,
                        "tier":        get_tier(score),
                        "hrProb":      hr_probability(score),
                        "parkFactor":  home_pf,
                        "sprintSpeed": sprint_map.get(name),
                        "ppLine":      pp_map.get(name, {}).get("line"),
                        "note":        "Base score: home park, neutral pitcher, calm conditions"
                    })
                results.sort(key=lambda x: -x["score"])
                self.send_json(results[:10])
            except Exception as e:
                traceback.print_exc()
                self.send_json({"error": str(e)}, 500)

        elif path == "/api/refresh":
            with _cache_lock:
                _cache.clear()
            self.send_json({"ok": True, "message": "Cache cleared"})

        elif path.endswith(".html"):
            self.send_file(path.lstrip("/"), "text/html; charset=utf-8")

        elif path.endswith(".js"):
            self.send_file(path.lstrip("/"), "application/javascript")

        elif path.endswith(".css"):
            self.send_file(path.lstrip("/"), "text/css")

        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()

# ── STARTUP ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import webbrowser, sys

    local = HOST in ("0.0.0.0", "127.0.0.1")
    url = f"http://localhost:{PORT}" if local else f"http://{HOST}:{PORT}"
    print(f"""
  ⬥ DIAMOND HR PROP BOARD — Live Server
  ─────────────────────────────────────────
  Starting on {url}
  Listening on {HOST}:{PORT}
  Player search: {url}/api/search?q=Aaron+Judge

  Data refreshes every {CACHE_TTL // 60} minutes.
  Press Ctrl+C to stop.
  ─────────────────────────────────────────
""")

    server = HTTPServer((HOST, PORT), Handler)

    # Pre-warm cache in background
    def warm():
        try:
            date_str, _ = get_today_str()
            build_daily_data(date_str)
        except Exception as e:
            print(f"[warm] {e}")
    threading.Thread(target=warm, daemon=True).start()

    # Open browser after short delay (local only)
    if local:
        def open_browser():
            time.sleep(1.5)
            webbrowser.open(f"http://localhost:{PORT}")
        threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
        sys.exit(0)
