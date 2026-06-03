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

import json, time, threading, traceback, difflib, os
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.error import URLError

# ── CONFIG ───────────────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 8765))
HOST = os.environ.get("HOST", "0.0.0.0")
CACHE_TTL = 900          # 15 minutes
USER_AGENT = "DiamondHRBoard/1.0"

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

def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < CACHE_TTL:
            return entry["data"]
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {"ts": time.time(), "data": data}

# ── HTTP HELPER ──────────────────────────────────────────────────────────────
def fetch(url, timeout=10):
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())

# ── MLB STATS API ─────────────────────────────────────────────────────────────
def get_today_str():
    et = datetime.now(timezone(timedelta(hours=-4)))  # Eastern Time
    return et.strftime("%Y-%m-%d"), et.hour

def fetch_schedule(date_str):
    cached = cache_get(f"schedule_{date_str}")
    if cached: return cached

    url = (f"https://statsapi.mlb.com/api/v1/schedule"
           f"?sportId=1&date={date_str}"
           f"&hydrate=probablePitcher,lineups,team,venue,weather,linescore")
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

                games.append({
                    "id": f"{away.lower()}-{home.lower()}-{g['gamePk']}",
                    "gamePk": g["gamePk"],
                    "away": away, "home": home,
                    "time": time_str,
                    "venue": venue_name,
                    "parkFactor": pf,
                    "isDome": dome,
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
    """Fetch current IL from MLB transactions API"""
    cached = cache_get("injuries")
    if cached: return cached

    url = "https://statsapi.mlb.com/api/v1/transactions?sportId=1&limit=500&transactionType=IL"
    try:
        data = fetch(url)
        injured = set()
        for t in data.get("transactions", []):
            if t.get("typeCode") in ("IL10","IL15","IL60","DL10","DL15","DL60"):
                pname = t.get("player", {}).get("fullName")
                # Only add if no toDate (still on IL)
                if pname and not t.get("toDate"):
                    injured.add(pname)
        # Also pull from active roster IL
        il_url = "https://statsapi.mlb.com/api/v1/teams?sportId=1&hydrate=roster(rosterType=fullRoster)"
        try:
            team_data = fetch(il_url)
            for team in team_data.get("teams", []):
                for player in team.get("roster", []):
                    if player.get("status", {}).get("code") in ("IL10","IL15","IL60","DL"):
                        injured.add(player["person"]["fullName"])
        except:
            pass
        cache_set("injuries", injured)
        return injured
    except Exception as e:
        print(f"[injuries] Error: {e}")
        # Return known injuries as fallback
        return {
            "Elly De La Cruz","Francisco Lindor","Luis Robert Jr.","Tarik Skubal",
            "Gleyber Torres","Javier Báez","Corey Seager","Wyatt Langford",
            "Ronald Acuña Jr.","Ryan Jeffers","Zach Neto","Bailey Ober",
            "Sean Murphy","Garrett Crochet"
        }

def fetch_batting_stats_savant():
    """
    Fetch season Statcast batting stats from Baseball Savant CSV endpoint.
    Returns dict keyed by player name.
    """
    cached = cache_get("savant_batting")
    if cached: return cached

    url = (
        "https://baseballsavant.mlb.com/leaderboard/statcast"
        "?type=batter&year=2026&position=&team=&min=25"
        "&csv=true"
    )
    try:
        req = Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv"
        })
        with urlopen(req, timeout=15) as r:
            raw = r.read().decode("utf-8")

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
                    "barrel_pct": float(row.get("barrel_batted_rate", 0) or 0),
                    "hard_hit_pct": float(row.get("hard_hit_percent", 0) or 0),
                    "avg_ev": float(row.get("avg_hit_speed", 0) or 0),
                    "xwoba": float(row.get("xwoba", 0) or 0),
                    "xslg": float(row.get("xslg", 0) or 0),
                    "sweet_spot_pct": float(row.get("sweet_spot_percent", 0) or 0),
                    "launch_angle": float(row.get("avg_launch_angle", 0) or 0),
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
    """Fetch all active MLB players (40-man roster) — names + team + basic stats"""
    cached = cache_get("rosters")
    if cached: return cached

    url = ("https://statsapi.mlb.com/api/v1/sports/1/players"
           "?season=2026&gameType=R")
    try:
        data = fetch(url)
        players = {}
        for p in data.get("people", []):
            pid = p.get("id")
            name = p.get("fullName", "")
            team_abbr = p.get("currentTeam", {}).get("abbreviation", "")
            team = TEAM_MAP.get(team_abbr, team_abbr)
            pos = p.get("primaryPosition", {}).get("abbreviation", "")
            bats = p.get("batSide", {}).get("code", "R")
            players[name] = {
                "id": pid, "team": team, "pos": pos,
                "bats": bats, "name": name
            }
        cache_set("rosters", players)
        print(f"[rosters] Loaded {len(players)} players")
        return players
    except Exception as e:
        print(f"[rosters] Error: {e}")
        return {}

def fetch_batting_season_stats():
    """Fetch season batting stats (HR, AVG, OPS, SLG) from MLB Stats API"""
    cached = cache_get("batting_stats")
    if cached: return cached

    url = ("https://statsapi.mlb.com/api/v1/stats"
           "?stats=season&group=hitting&season=2026&sportId=1"
           "&limit=500&sortStat=homeRuns&order=desc")
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
            result[name] = {
                "G": g,
                "HR": hr,
                "AVG": float(stat.get("avg", ".000").replace(".","0.") if "." in str(stat.get("avg","")) else 0),
                "OPS": float(stat.get("ops", ".000").replace(".","0.") if stat.get("ops") else 0),
                "SLG": float(stat.get("slg", ".000").replace(".","0.") if stat.get("slg") else 0),
                "OBP": float(stat.get("obp", ".000").replace(".","0.") if stat.get("obp") else 0),
                "ISO": 0.0,  # calculated below
                "hrPct": round((hr / g) * 100, 1) if g > 0 else 0,
            }
            avg = result[name]["AVG"]
            slg = result[name]["SLG"]
            result[name]["ISO"] = round(slg - avg, 3)

        cache_set("batting_stats", result)
        print(f"[batting] Loaded {len(result)} batters")
        return result
    except Exception as e:
        print(f"[batting] Error: {e}")
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

def calc_composite(batter_stats, savant_stats, pitcher_stats, pf, wx):
    hr_pct    = batter_stats.get("hrPct", 0)
    ops       = batter_stats.get("OPS", 0)
    slg       = batter_stats.get("SLG", 0)
    iso       = batter_stats.get("ISO", 0)
    barrel    = savant_stats.get("barrel_pct", 8)
    hard_hit  = savant_stats.get("hard_hit_pct", 40)
    sweet     = savant_stats.get("sweet_spot_pct", 36)
    bats      = batter_stats.get("bats", "R")
    ph        = pitcher_stats.get("hand", "R")

    s = 0
    s += min(hr_pct * 2.3, 27)
    s += min((ops - 0.600) * 36, 20)
    s += min((slg - 0.350) * 30, 15)
    s += min((barrel - 8) * 0.7, 8)
    s += min((hard_hit - 40) * 0.22, 5)
    s += min((iso - 0.180) * 18, 4)
    s += min((sweet - 36) * 0.2, 2)
    s += pitcher_vuln(pitcher_stats) * 0.17
    s += ((pf - 100) / 50) * 8
    s += wind_adj(wx) * 0.48
    s += temp_adj(wx.get("temp", 72))
    s += baro_adj(wx.get("pressure", 1013))
    s += humid_adj(wx.get("humidity", 55))
    s += platoon_adj(bats, ph)
    return max(0, min(99, round(s)))

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
    games       = fetch_schedule(date_str)
    batting     = fetch_batting_season_stats()
    savant      = fetch_batting_stats_savant()
    rosters     = fetch_active_rosters()
    injuries    = fetch_injuries()
    _, et_hour  = get_today_str()

    # Enrich games with pitcher stats + weather + player projections
    for g in games:
        home = g["home"]

        # Game hour from time string
        try:
            t = g["time"].replace(" ET", "").replace(" PM", "").replace(" AM", "")
            hour = int(t.split(":")[0])
            is_pm = "PM" in g["time"]
            if is_pm and hour != 12: hour += 12
            if not is_pm and hour == 12: hour = 0
        except:
            hour = 19  # default 7pm

        wx = fetch_weather(home, hour)
        g["weather"] = wx

        # Pitcher stats
        for side in ["awayPitcher", "homePitcher"]:
            pid = g[side].get("id")
            stats = fetch_pitcher_stats(pid) if pid else {}
            g[side].update(stats)
            if not stats:
                g[side].update({"era": 4.50, "quality": "mid", "fbPct": 38, "vel": 92.5})

        pf = g["parkFactor"]

        # Build player projections for this game
        players = []
        game_teams = {g["away"], g["home"]}

        for name, roster_info in rosters.items():
            if name in injuries: continue
            team = roster_info.get("team", "")
            if team not in game_teams: continue
            pos = roster_info.get("pos", "")
            if pos in ("P", "SP", "RP"): continue  # skip pitchers

            bat_stat = batting.get(name, {})
            if bat_stat.get("G", 0) < 5: continue  # skip players with too few games

            sav = savant.get(name, {})
            is_away = team == g["away"]
            opp_pitcher = g["homePitcher"] if is_away else g["awayPitcher"]

            bat_stat["bats"] = roster_info.get("bats", "R")
            score = calc_composite(bat_stat, sav, opp_pitcher, pf, wx)
            tier  = get_tier(score)

            players.append({
                "name":      name,
                "team":      team,
                "pos":       pos,
                "bats":      roster_info.get("bats", "R"),
                "G":         bat_stat.get("G", 0),
                "HR":        bat_stat.get("HR", 0),
                "hrPct":     bat_stat.get("hrPct", 0),
                "AVG":       bat_stat.get("AVG", 0),
                "OPS":       bat_stat.get("OPS", 0),
                "SLG":       bat_stat.get("SLG", 0),
                "ISO":       bat_stat.get("ISO", 0),
                "barrel":    sav.get("barrel_pct", 0),
                "hardHit":   sav.get("hard_hit_pct", 0),
                "avgEV":     sav.get("avg_ev", 0),
                "xwOBA":     sav.get("xwoba", 0),
                "sweetSpot": sav.get("sweet_spot_pct", 0),
                "pitcher":   opp_pitcher.get("name", "TBD"),
                "pitcherEra":opp_pitcher.get("era", 4.50),
                "pitcherHand":opp_pitcher.get("hand", "R"),
                "gameId":    g["id"],
                "score":     score,
                "tier":      tier,
            })

        players.sort(key=lambda x: x["score"], reverse=True)
        g["players"] = players
        g["topPick"] = players[0] if players else None

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
                rosters  = fetch_active_rosters()
                batting  = fetch_batting_season_stats()
                savant   = fetch_batting_stats_savant()
                injuries = fetch_injuries()

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
                        "onIL":       name in injuries,
                        "score":      score,
                        "tier":       get_tier(score),
                        "parkFactor": home_pf,
                        "note":       "Base score: home park, neutral pitcher, calm conditions"
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
    import webbrowser, os, sys

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

    # Open browser after short delay
    def open_browser():
        time.sleep(1.5)
        webbrowser.open(f"http://localhost:{PORT}")
    threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
        sys.exit(0)
