"""
scrape_soccerway.py
--------------------
Scrapes a Soccerway match page across 3 tabs and saves data to CSV.

REQUIREMENTS:
    pip install selenium webdriver-manager pandas beautifulsoup4

USAGE:
    python scrape_soccerway.py
    python scrape_soccerway.py --url "https://us.soccerway.com/game/..." --output my_match.csv
    python scrape_soccerway.py --debug    # saves raw HTML for troubleshooting

PAGES SCRAPED:
    1. Summary  → league, round, season, date, time, teams, score,
                  attendance, capacity, stadium, city
    2. Stats    → shots, corners, fouls, cards, offsides, etc.
    3. Lineups  → home/away formation, home/away team rating

COLUMN ORDER IN OUTPUT CSV:
    league_division, season, round, date, attendance, capacity,
    stadium, city, kickoff_time, home_team, away_team,
    home_goals, away_goals, result,
    home_formation, away_formation,
    home_team_rating, away_team_rating,
    ... all stats ...
"""

import argparse
import csv
import os
import random
import re
import time
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager


# ─── Driver ───────────────────────────────────────────────────────────────────

def build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def get_page_html(driver, url: str, extra_wait: float = 3.0) -> str:
    """
    Navigate to URL and wait until the page has meaningful content.

    Strategy (fastest safe approach):
      1. Navigate and give a short fixed floor (1s) for the initial JS boot.
      2. Poll every 0.3s until one of the known content markers is present,
         or until max_wait (8s) is reached.
      3. Add a small random jitter (0.2-0.8s) after content is detected to
         look more human and let any lazy-loaded widgets finish.

    Content markers cover all three tabs (summary, stats, lineups).
    """
    driver.get(url)
    time.sleep(1.0)  # hard floor — let initial JS execute

    CONTENT_SELECTORS = [
        "[class*='wcl-homeValue']",                  # stats tab
        "[class*='detailScore']",                    # score on summary
        "[class*='participant__participantName']",   # team names
        "[class*='formation']",                      # lineups tab
        "[class*='wcl-category_']",                  # any wcl stat row
    ]

    max_wait   = 8.0
    poll_every = 0.3
    elapsed    = 0.0

    while elapsed < max_wait:
        for sel in CONTENT_SELECTORS:
            try:
                if driver.find_elements(By.CSS_SELECTOR, sel):
                    # Content found — add small human jitter then return
                    time.sleep(random.uniform(0.2, 0.8))
                    return driver.page_source
            except Exception:
                pass
        time.sleep(poll_every)
        elapsed += poll_every

    # Timeout reached — return whatever we have
    return driver.page_source


# ─── URL builders ─────────────────────────────────────────────────────────────

def get_base_path(url: str) -> tuple[str, str]:
    """
    Split URL into (base_game_path, query_string).
    e.g. "https://us.soccerway.com/game/arsenal-.../wolves-.../?mid=X"
         → ("https://us.soccerway.com/game/arsenal-.../wolves-.../", "?mid=X")
    """
    qs = ""
    if "?" in url:
        path, q = url.split("?", 1)
        qs = "?" + q
    else:
        path = url

    m = re.match(r"(https?://[^/]+/game/[^/]+/[^/]+/)", path)
    base = m.group(1) if m else path.rstrip("/") + "/"
    return base, qs


def build_stats_url(base_url: str) -> str:
    base, qs = get_base_path(base_url)
    return base + "summary/stats/overall/" + qs


def build_lineups_url(base_url: str) -> str:
    base, qs = get_base_path(base_url)
    return base + "summary/lineups/" + qs


# ─── Shared utilities ─────────────────────────────────────────────────────────

def result_code(home: str, away: str) -> str:
    try:
        h, a = int(home), int(away)
        return "H" if h > a else ("A" if h < a else "D")
    except (ValueError, TypeError):
        return ""


def clean_number(raw: str) -> str:
    """Remove all non-digit characters to get a clean integer string."""
    return re.sub(r"\D", "", raw)


def jitter_sleep(min_s: float, max_s: float) -> float:
    """Sleep a random duration between min_s and max_s. Returns actual seconds slept."""
    duration = random.uniform(min_s, max_s)
    time.sleep(duration)
    return duration


def extract_match_id(url: str) -> str:
    """
    Extract the match ID from the ?mid= query param.
    e.g. https://.../?mid=M1w8YmqE  →  "M1w8YmqE"
    Falls back to extracting any alphanumeric segment after 'mid=' if needed.
    """
    m = re.search(r"[?&]mid=([A-Za-z0-9]+)", url)
    return m.group(1) if m else ""


# def extract_season(url: str, match_date: str = "") -> str:
#     """
#     Option C — URL-first, then date-based fallback.

#     1. Try to extract an explicit season from the URL (e.g. "2024-2025").
#     2. If not found, derive from the match date:
#          - Matches played Aug–Dec  → season = YEAR-(YEAR+1)
#          - Matches played Jan–Jul  → season = (YEAR-1)-YEAR
#     3. If no date either, fall back to current season heuristic.

#     e.g. .../premier-league-2024-2025/...  →  "2024-2025"  (from URL)
#          date="15/03/2024"                →  "2023-2024"  (from date)
#          date="25/09/2023"                →  "2023-2024"  (from date)
#     """
#     # 1. Try URL first
#     m = re.search(r"(\d{4}-\d{4})", url)
#     if m:
#         return m.group(1)

#     # 2. Derive from match_date (supports dd/mm/yyyy or mm/dd/yyyy or yyyy-mm-dd)
#     if match_date:
#         # Try dd/mm/yyyy or mm/dd/yyyy
#         dm = re.search(r"(\d{2})/(\d{2})/(\d{4})", match_date)
#         if dm:
#             day_or_month, month_or_day, year = int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
#             # Soccerway uses dd/mm/yyyy
#             month = month_or_day
#             if 8 <= month <= 12:
#                 return f"{year}-{year + 1}"
#             else:
#                 return f"{year - 1}-{year}"
#         # Try yyyy-mm-dd
#         dm2 = re.search(r"(\d{4})-(\d{2})-(\d{2})", match_date)
#         if dm2:
#             year, month = int(dm2.group(1)), int(dm2.group(2))
#             if 8 <= month <= 12:
#                 return f"{year}-{year + 1}"
#             else:
#                 return f"{year - 1}-{year}"

#     # 3. Last resort: compute from today
#     today = datetime.now()
#     if today.month >= 8:
#         return f"{today.year}-{today.year + 1}"
#     else:
#         return f"{today.year - 1}-{today.year}"


# ─── Summary page parser ──────────────────────────────────────────────────────

def parse_summary(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    data = {}

    title_tag = soup.find("title")
    page_title = title_tag.text.strip() if title_tag else ""
    # e.g. "Arsenal v Wolves 28/05/2023 - Soccerway.com"
    body_text = soup.get_text(" ", strip=True)

    # ── League + Round ────────────────────────────────────────────────────────
    # Breadcrumb gives e.g.:
    #   "Premier League - Round 30"
    #   "Conference League - Play Offs - Final"
    #   "Conference League - Play Offs - Quarter-finals"
    #   "Conference League - Play Offs - 1/8-finals"
    breadcrumbs = soup.select("nav[data-testid='wcl-breadcrumbs'] ol li a span")
    raw_league = ""
    if breadcrumbs and len(breadcrumbs) > 1:
        raw_league = breadcrumbs[1].text.strip()

    # Known knockout-stage labels (longest first to avoid prefix collisions)
    KNOCKOUT_STAGES = [
        "1/16-finals", "1/8-finals", "quarter-finals", "semi-finals",
        "third place", "final",
    ]

    # 1. Numbered round: "Premier League - Round 30" / "Matchday 5" / etc.
    round_m = re.search(
        r"[-\u2013\xb7|]\s*(?:Round|Matchday|GW|Week|Jornada|Spieltag|Giornata|Journ\xe9e)\s*(\d+)",
        raw_league, re.IGNORECASE
    )
    if round_m:
        data["league_division"] = raw_league[:round_m.start()].strip()
        data["round"] = round_m.group(1)
    else:
        # 2. Knockout stage label anywhere in the string
        knockout_found = None
        for stage in KNOCKOUT_STAGES:
            if re.search(re.escape(stage), raw_league, re.IGNORECASE):
                knockout_found = stage
                break

        if knockout_found:
            # league_division = everything before the first " - " separator
            parts = re.split(r"\s*-\s*", raw_league, maxsplit=1)
            data["league_division"] = parts[0].strip()
            data["round"] = knockout_found.capitalize()
        else:
            # 3. Plain trailing number: "Premier League 30"
            trail_m = re.search(r"\s+(\d+)$", raw_league)
            if trail_m:
                data["league_division"] = raw_league[:trail_m.start()].strip()
                data["round"] = trail_m.group(1)
            else:
                data["league_division"] = raw_league
                data["round"] = ""

    # ── Date ─────────────────────────────────────────────────────────────────
    # Title usually contains "28/05/2023"
    date_m = re.search(r"(\d{2}/\d{2}/\d{4}|[A-Za-z]+ \d{2}, \d{4})", page_title + " " + body_text)
    data["date"] = date_m.group(1) if date_m else ""

    # ── Kickoff time ──────────────────────────────────────────────────────────
    time_val = ""
    for sel in [".duelParticipant__startTime", ".event__time", ".match-time",
                "[class*='startTime']", "[class*='kickoff']"]:
        el = soup.select_one(sel)
        if el:
            t = el.text.strip()
            if re.search(r"\d{1,2}:\d{2}", t):
                if "," in t:
                    time_part, date_part = [x.strip() for x in t.split(",", 1)]
                    time_val = time_part
                    data["date"] = date_part   # ← overwrite date here
                else:
                    time_val = t
                break
    # Fallback: scan body for a time pattern that isn't part of a date
    if not time_val:
        # Avoid matching date parts like "28/05/2023"
        for m in re.finditer(r"\b(\d{1,2}:\d{2})\b", body_text):
            candidate = m.group(1)
            # Exclude anything that looks like a score (single digit : single digit)
            if len(candidate) >= 4:
                time_val = candidate
                break
    data["kickoff_time"] = time_val

    # ── Teams ─────────────────────────────────────────────────────────────────
    home, away = "", ""
    for sel in [".duelParticipant__home .participant__participantName",
                ".home-team .team-name", "[class*='HomeTeam'] [class*='name']"]:
        el = soup.select_one(sel)
        if el and el.text.strip():
            home = el.text.strip()
            break
    for sel in [".duelParticipant__away .participant__participantName",
                ".away-team .team-name", "[class*='AwayTeam'] [class*='name']"]:
        el = soup.select_one(sel)
        if el and el.text.strip():
            away = el.text.strip()
            break

    # Reliable fallback: "Arsenal v Wolves 28/05/2023"
    if not home or not away:
        m = re.match(r"^(.+?)\s+v\s+(.+?)(?:\s+\d{2}/|\s*[-|]|\s*Lineups|\s*Stats)",
                     page_title, re.IGNORECASE)
        if m:
            home = home or m.group(1).strip()
            away = away or m.group(2).strip()

    data["home_team"] = home
    data["away_team"] = away

    # ── Full-time score ───────────────────────────────────────────────────────
    ft_home, ft_away = "", ""
    score_el = soup.select_one(
        ".detailScore__wrapper, .scoreboard, .current-result, "
        "[class*='scoreBox'], [class*='score-board']"
    )
    if score_el:
        nums = re.findall(r"\b(\d+)\b", score_el.text)
        if len(nums) >= 2:
            ft_home, ft_away = nums[0], nums[1]

    # Title fallback: "Arsenal v Wolves 5:0" or "Arsenal - Wolves 5:0"
    if not ft_home:
        sm = re.search(r"(\d+)\s*[:\-]\s*(\d+)", page_title)
        if sm:
            ft_home, ft_away = sm.group(1), sm.group(2)

    data["home_goals"] = ft_home
    data["away_goals"] = ft_away
    data["result"] = result_code(ft_home, ft_away)

    # ── Attendance ────────────────────────────────────────────────────────────
    # The number may be written as "60 201" (with a space) — strip all non-digits
    att_m = re.search(
        r"Attendance[:\s]+([\d][\d\s,\.]*)",
        body_text, re.IGNORECASE
    )
    if att_m:
        raw_att = att_m.group(1).split("\n")[0].strip()   # stop at newline
        data["attendance"] = clean_number(raw_att)
    else:
        data["attendance"] = ""

    # ── Capacity ──────────────────────────────────────────────────────────────
    cap_m = re.search(
        r"Capacity[:\s]+([\d][\d\s,\.]*)",
        body_text, re.IGNORECASE
    )
    if cap_m:
        raw_cap = cap_m.group(1).split("\n")[0].strip()
        data["capacity"] = clean_number(raw_cap)
    else:
        data["capacity"] = ""

    # ── Stadium + City ────────────────────────────────────────────────────────
    # The venue info block renders as two adjacent <span>s inside a wcl-infoValue div:
    #   <span>Allwyn Arena</span><span> (Athens)</span>
    data["stadium"] = ""
    data["city"]    = ""
    # NEW: Find the Venue info block using label + value structure
    for info_block in soup.select("div[data-testid='wcl-summaryMatchInformation'] > div"):
        label_wrapper = info_block.select_one("div[class*='wcl-infoLabelWrapper']")
        if label_wrapper and "venue" in label_wrapper.get_text(strip=True).lower():
            value_div = info_block.find_next_sibling("div")
            if value_div and "wcl-infoValue" in value_div.get("class", [""])[0]:
                spans = value_div.find_all("span", recursive=False)
                if len(spans) >= 1:
                    data["stadium"] = spans[0].get_text(strip=True)
                    if len(spans) >= 2:
                        city_candidate = spans[1].get_text(strip=True)
                        if re.match(r"^\((.+)\)$", city_candidate):
                            data["city"] = city_candidate.strip("()")
                break
    # Fallback: body text "Venue: Allwyn Arena (Athens)"
    if not data["stadium"]:
        vm = re.search(
            r"(?:Venue|Stadium)[:\s]+([^(\n]+)\s*\(([^)]+)\)",
            body_text, re.IGNORECASE
        )
        if vm:
            data["stadium"] = vm.group(1).strip()
            data["city"]    = vm.group(2).strip()

    return data


# ─── Stats page parser ────────────────────────────────────────────────────────

# STAT_MAP: label (lowercase) → (home_col, away_col)
# For stats with pct+counts format (e.g. "87% (541/623)"), the base col name is used
# and the parser will also populate _pct, _successful, _total variants automatically.
STAT_MAP = {
    # ── Shots ─────────────────────────────────────────────────────────────────
    "total shots":              ("home_shots_total",              "away_shots_total"),
    "shots on goal":            ("home_shots_on_target",          "away_shots_on_target"),
    "shots on target":          ("home_shots_on_target",          "away_shots_on_target"),
    "shots off target":         ("home_shots_off_target",         "away_shots_off_target"),
    "blocked shots":            ("home_shots_blocked",            "away_shots_blocked"),
    "shots outside the box":    ("home_shots_outside_box",        "away_shots_outside_box"),
    "shots inside the box":     ("home_shots_inside_box",         "away_shots_inside_box"),
    "hit woodwork":             ("home_hit_woodwork",             "away_hit_woodwork"),
    "hit the post":             ("home_hit_woodwork",             "away_hit_woodwork"),
    # ── Set pieces ────────────────────────────────────────────────────────────
    "corner kicks":             ("home_corners",                  "away_corners"),
    "corners":                  ("home_corners",                  "away_corners"),
    "free kicks":               ("home_free_kicks",               "away_free_kicks"),
    "throw ins":                ("home_throw_ins",                "away_throw_ins"),
    # ── Discipline ────────────────────────────────────────────────────────────
    "fouls":                    ("home_fouls_committed",          "away_fouls_committed"),
    "offsides":                 ("home_offsides",                 "away_offsides"),
    "yellow cards":             ("home_yellow_cards",             "away_yellow_cards"),
    "red cards":                ("home_red_cards",                "away_red_cards"),
    # ── Goalkeeper ────────────────────────────────────────────────────────────
    "goalkeeper saves":         ("home_goalkeeper_saves",         "away_goalkeeper_saves"),
    "goals prevented":          ("home_goals_prevented",          "away_goals_prevented"),
    # ── Possession ────────────────────────────────────────────────────────────
    "ball possession":          ("home_possession_pct",           "away_possession_pct"),
    "touches in opposition box":("home_touches_opp_box",          "away_touches_opp_box"),
    # ── Passing (pct+counts format) ───────────────────────────────────────────
    "passes in final third":    ("home_passes_final_third",       "away_passes_final_third"),
    "long passes":              ("home_long_passes",              "away_long_passes"),
    "passes":                   ("home_passes",                   "away_passes"),
    "accurate passes":          ("home_accurate_passes",          "away_accurate_passes"),
    "accurate through passes":  ("home_accurate_through_passes",  "away_accurate_through_passes"),
    "big chances":              ("home_big_chances",              "away_big_chances"),
    "headed goals":             ("home_headed_goals",             "away_headed_goals"),
    "crosses":                  ("home_crosses",                  "away_crosses"),
    "duels won":                ("home_duels_won",                "away_duels_won"),
    "errors leading to shot":   ("home_errors_leading_to_shot",   "away_errors_leading_to_shot"),
    "errors leading to goal":   ("home_errors_leading_to_goal",   "away_errors_leading_to_goal"),
    # ── Defending ─────────────────────────────────────────────────────────────
    "tackles":                  ("home_tackles",                  "away_tackles"),
    "clearances":               ("home_clearances",               "away_clearances"),
    "interceptions":            ("home_interceptions",            "away_interceptions"),
    # attacks / dangerous attacks removed — not served on Soccerway stats page
    # ── Expected stats (decimal values) ───────────────────────────────────────
    "expected goals (xg)":      ("home_xg",                      "away_xg"),
    "expected goals":           ("home_xg",                      "away_xg"),
    "xg":                       ("home_xg",                      "away_xg"),
    "xg on target (xgot)":      ("home_xgot",                    "away_xgot"),
    "xg on target":             ("home_xgot",                    "away_xgot"),
    "xgot faced":               ("home_xgot_faced",              "away_xgot_faced"),
    "expected assists (xa)":    ("home_xa",                      "away_xa"),
    "expected assists":         ("home_xa",                      "away_xa"),
}

# Stats that use the "87% (541/623)" format — parser will split into _pct / _successful / _total
PCT_COUNT_STATS = {
    "home_passes", "away_passes",
    "home_long_passes", "away_long_passes",
    "home_passes_final_third", "away_passes_final_third",
    "home_tackles", "away_tackles",
    "home_crosses", "away_crosses",
}

# Stats that can be negative (e.g. goals_prevented = -0.42)
SIGNED_DECIMAL_STATS = {
    "home_goals_prevented", "away_goals_prevented",
    "home_xg", "away_xg",
    "home_xgot", "away_xgot",
    "home_xgot_faced", "away_xgot_faced",
    "home_xa", "away_xa",
}

ALL_STAT_COLS = sorted({col for pair in STAT_MAP.values() for col in pair})

# Expand ALL_STAT_COLS to include _pct / _successful / _total variants
_EXPANDED = []
for _col in ALL_STAT_COLS:
    _EXPANDED.append(_col)
    if _col in PCT_COUNT_STATS:
        _EXPANDED += [_col + "_pct", _col + "_successful", _col + "_total"]
ALL_STAT_COLS = _EXPANDED


def _parse_value(raw: str, col_name: str) -> str:
    """
    Clean a raw stat value string to its final stored form.
    - Signed decimal stats (xG, goals_prevented etc): keep sign + decimals
    - Percentage stats: strip % symbol, keep number
    - Everything else: strip non-digit characters
    """
    raw = raw.strip()
    if col_name in SIGNED_DECIMAL_STATS:
        m = re.search(r"-?\d+\.?\d*", raw)
        return m.group(0) if m else raw
    # Remove % but keep the number (including decimals for possession)
    raw = raw.replace("%", "").strip()
    return raw


def _parse_pct_count(raw: str) -> tuple[str, str, str]:
    """
    Parse "87% (541/623)" into ("87", "541", "623").
    Returns ("", "", "") if format doesn't match.
    """
    m = re.search(r"(\d+)\s*%?\s*\((\d+)/(\d+)\)", raw)
    if m:
        return m.group(1), m.group(2), m.group(3)
    # Sometimes just "87%" with no counts
    m2 = re.search(r"(\d+)\s*%", raw)
    if m2:
        return m2.group(1), "", ""
    return "", "", ""


def try_assign(data: dict, label: str, home_raw: str, away_raw: str):
    """
    Match a stat label to known columns and write values into data.
    Handles plain numbers, percentages, pct+count format, and signed decimals.
    Keys are checked longest-first so specific keys (e.g. "xgot faced") always
    win over shorter substrings (e.g. "xg").
    """
    label = label.lower().strip()
    for key, (hk, ak) in sorted(STAT_MAP.items(), key=lambda x: -len(x[0])):
        if key in label:
            if data.get(hk) not in ("", None, "0"):
                return  # already populated, don't overwrite

            if hk in PCT_COUNT_STATS:
                # home
                h_pct, h_succ, h_total = _parse_pct_count(home_raw)
                data[hk] = h_pct  # base col = pct (empty if not available)
                if not h_pct and h_total:
                    data[hk] = h_total  # fallback: store total count if no pct
                data[hk + "_pct"] = h_pct
                data[hk + "_successful"] = h_succ
                data[hk + "_total"] = h_total
                # away
                a_pct, a_succ, a_total = _parse_pct_count(away_raw)
                data[ak] = a_pct
                data[ak + "_pct"] = a_pct
                data[ak + "_successful"] = a_succ
                data[ak + "_total"] = a_total
                if not a_pct and a_total:
                    data[ak] = a_total  # fallback: store total count if no pct
            else:
                data[hk] = _parse_value(home_raw, hk)
                data[ak] = _parse_value(away_raw, ak)
            return


def _extract_stat_row(row_el) -> tuple[str, str, str] | None:
    """
    Given a BeautifulSoup element that represents one stat row, extract
    (home_value, label, away_value).

    Soccerway uses the wcl-* class structure:
        <div class="wcl-category_...">
            <div class="wcl-value_... wcl-homeValue_...">  ← home
            <div class="wcl-category_... (inner)">         ← label
            <div class="wcl-value_... wcl-awayValue_...">  ← away
        </div>

    The home value block may contain two <span>s: "87%" and "(541/623)".
    We join them so try_assign can parse the combined string.
    """
    # Primary: wcl-* selectors from the actual HTML structure
    home_el  = row_el.select_one("[class*='wcl-homeValue']")
    label_el = row_el.select_one("[data-testid='wcl-statistics-category'],"
                                  "[class*='wcl-category_6']")   # inner category div
    away_el  = row_el.select_one("[class*='wcl-awayValue']")

    if home_el and label_el and away_el:
        # Join all text spans within the value block (e.g. "87%" + "(541/623)")
        home_val  = " ".join(s.strip() for s in home_el.stripped_strings)
        away_val  = " ".join(s.strip() for s in away_el.stripped_strings)
        label_txt = " ".join(s.strip() for s in label_el.stripped_strings)
        return home_val, label_txt, away_val

    # Fallback: generic 3-part text split
    parts = [t.strip() for t in row_el.stripped_strings if t.strip()]
    if len(parts) >= 3:
        # For pct+count rows there will be more than 3 parts e.g.:
        # ["87%", "(541/623)", "Passes", "76%", "(214/283)"]
        # Reconstruct home and away by splitting around the label
        mid = len(parts) // 2
        label_txt = parts[mid]
        home_val = " ".join(parts[:mid])
        away_val = " ".join(parts[mid + 1:])
        return home_val, label_txt, away_val

    return None


def parse_stats(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # Initialise all stat cols to ""; cards default to "0"
    data = {col: "" for col in ALL_STAT_COLS}
    data["home_yellow_cards"] = "0"
    data["away_yellow_cards"] = "0"
    data["home_red_cards"]    = "0"
    data["away_red_cards"]    = "0"

    # ── Strategy 1: wcl-category rows (actual Soccerway/Flashscore structure) ─
    # The outer stat row wrapper has class matching "wcl-category_" but NOT
    # the inner label wrapper. We distinguish them by checking for child
    # homeValue/awayValue divs.
    wcl_rows = soup.select("div[class*='wcl-category_']")
    # Keep only rows that contain both a homeValue and awayValue child
    wcl_rows = [r for r in wcl_rows
                if r.select_one("[class*='wcl-homeValue']")
                and r.select_one("[class*='wcl-awayValue']")]

    if wcl_rows:
        print(f"  [stats] {len(wcl_rows)} wcl-category rows found.")
        for row in wcl_rows:
            result = _extract_stat_row(row)
            if result:
                home_val, label, away_val = result
                try_assign(data, label, home_val, away_val)

    # ── Strategy 2: other common stat row class names ─────────────────────────
    if not any(v not in ("", "0") for v in data.values()):
        other_selectors = [
            "div[class*='statRow']",
            "div[class*='stat-row']",
            "div[class*='statistic__row']",
            "div[class*='statistics__item']",
            "li[class*='stat']",
            "tr[class*='stat']",
        ]
        rows = []
        for sel in other_selectors:
            rows = soup.select(sel)
            if rows:
                print(f"  [stats] {len(rows)} rows via fallback selector: {sel}")
                break
        for row in rows:
            result = _extract_stat_row(row)
            if result:
                home_val, label, away_val = result
                try_assign(data, label, home_val, away_val)

    # ── Strategy 3: generic HTML tables ───────────────────────────────────────
    if not any(v not in ("", "0") for v in data.values()):
        print("  [stats] Falling back to table scan...")
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) == 3:
                    try_assign(data, cells[1], cells[0], cells[2])

    # ── Strategy 4: line-by-line text scan ────────────────────────────────────
    if not any(v not in ("", "0") for v in data.values()):
        print("  [stats] Falling back to text line scan...")
        lines = [l.strip() for l in soup.get_text("\n").splitlines() if l.strip()]
        for i, line in enumerate(lines):
            if re.fullmatch(r"-?\d+\.?\d*%?", line) and i + 2 < len(lines):
                if re.fullmatch(r"-?\d+\.?\d*%?", lines[i + 2]):
                    try_assign(data, lines[i + 1], line, lines[i + 2])

    found = {k: v for k, v in data.items() if v not in ("", "0")}
    if found:
        print(f"  [stats] Captured {len(found)} non-default stat values.")
    else:
        print("  [stats] WARNING: No stats extracted. Run with --debug to inspect HTML.")

    return data


# ─── Lineups page parser ──────────────────────────────────────────────────────

def parse_lineups(html: str) -> dict:
    """
    Extract from the lineups tab:
      - home_formation  e.g. "4-3-3"
      - away_formation  e.g. "3-4-3"
      - home_team_rating  e.g. "7.2"
      - away_team_rating  e.g. "6.1"
    """
    soup = BeautifulSoup(html, "html.parser")
    data = {
        "home_formation": "",
        "away_formation": "",
        "home_team_rating": "",
        "away_team_rating": "",
    }

    body_text = soup.get_text(" ", strip=True)

    # ── Formation ─────────────────────────────────────────────────────────────
    # Formations look like "4-3-3" or "4-2-3-1" or "3-5-2"
    formation_pattern = r"\b(\d\s*-\s*\d\s*-\s*\d(?:\s*-\s*\d)?)\b"

    # Try dedicated formation elements first
    formation_selectors = [
        "[class*='formation']",
        "[class*='Formation']",
        "[data-formation]",
        ".lineup__formation",
    ]

    formations_found = []
    for sel in formation_selectors:
        els = soup.select(sel)
        for el in els:
            m = re.search(formation_pattern, el.text)
            if m:
                formation = re.sub(r"\s+", "", m.group(1))  # remove spaces
                formations_found.append(formation)
        if len(formations_found) >= 2:
            break

    # Fallback: scan all text for formation patterns
    if len(formations_found) < 2:
        raw_forms = re.findall(formation_pattern, body_text)
        # De-duplicate while preserving order
        formations_found = [re.sub(r"\s+", "", f) for f in raw_forms]

    if len(formations_found) >= 1:
        data["home_formation"] = formations_found[0]
    if len(formations_found) >= 2:
        data["away_formation"] = formations_found[1]

    # ── Team ratings ──────────────────────────────────────────────────────────
    # Ratings appear as decimal numbers like "7.2" near "rating" or as overall averages
    rating_selectors = [
        "[class*='rating']",
        "[class*='teamRating']",
        "[class*='team-rating']",
        "[class*='averageRating']",
        "[class*='average-rating']",
    ]

    ratings_found = []
    for sel in rating_selectors:
        els = soup.select(sel)
        for el in els:
            m = re.search(r"\b(\d+\.\d+)\b", el.text)
            if m:
                ratings_found.append(m.group(1))
        if len(ratings_found) >= 2:
            break

    # Fallback: look for "rating" keyword near a decimal
    if len(ratings_found) < 2:
        for m in re.finditer(
            r"(?:rating|avg|average)[^\d]{0,20}(\d+\.\d+)",
            body_text, re.IGNORECASE
        ):
            ratings_found.append(m.group(1))

    # Second fallback: find all decimal numbers in a plausible rating range (5.0 – 9.9)
    if len(ratings_found) < 2:
        candidates = re.findall(r"\b([5-9]\.\d)\b", body_text)
        seen_r = []
        for c in candidates:
            if c not in seen_r:
                seen_r.append(c)
        ratings_found = seen_r

    if len(ratings_found) >= 1:
        data["home_team_rating"] = ratings_found[0]
    if len(ratings_found) >= 2:
        data["away_team_rating"] = ratings_found[1]

    # Print what we found
    print(f"  [lineups] Home formation: {data['home_formation']} | "
          f"Away formation: {data['away_formation']}")
    print(f"  [lineups] Home rating: {data['home_team_rating']} | "
          f"Away rating: {data['away_team_rating']}")

    return data


# ─── Main orchestrator ────────────────────────────────────────────────────────

def _is_valid_soccerway_url(url: str) -> bool:
    """Return False if the URL doesn't look like a valid Soccerway match link."""
    return bool(re.search(r"https?://[a-z.]*soccerway\.com/game/", url, re.IGNORECASE))


def _early_exit(timing: dict, reason: str, match_start: float,
                t_summary: float = 0.0, t_stats: float = 0.0,
                t_lineups: float = 0.0) -> dict:
    """Populate timing dict for an early-exit failure and return it."""
    timing["success"]         = False
    timing["reason"]          = reason
    timing["t_summary_s"]     = t_summary
    timing["t_stats_s"]       = t_stats
    timing["t_lineups_s"]     = t_lineups
    timing["t_scrape_only_s"] = round(t_summary + t_stats + t_lineups, 2)
    timing["t_total_s"]       = round(time.perf_counter() - match_start, 2)
    timing["non_empty_fields"] = 0
    return timing


def scrape_match(
    base_url: str,
    driver: webdriver.Chrome,
    debug: bool = False,
) -> tuple[dict, dict]:
    """
    Scrape one match across 3 pages.

    timing["reason"] values:
      ""          → success (None written to log)
      "false"     → URL itself is not a valid Soccerway match link
      "404"       → page returned HTTP 404 / "not found"
      "403"       → page returned HTTP 403 / "forbidden"
      "no_data"   → page loaded but no team names found (broken page)
      "no_stats"  → stats tab returned no data at all
      "no_lineups"→ lineups tab returned no formation/rating data
      "else"      → any other unexpected exception
    """
    stats_url   = build_stats_url(base_url)
    lineups_url = build_lineups_url(base_url)

    print(f"\nSummary URL : {base_url}")
    print(f"Stats URL   : {stats_url}")
    print(f"Lineups URL : {lineups_url}\n")

    data   = {}
    timing = {
        "url":                   base_url,
        "t_summary_s":           0.0,
        "sleep_after_summary_s": 0.0,
        "t_stats_s":             0.0,
        "sleep_after_stats_s":   0.0,
        "t_lineups_s":           0.0,
        "t_scrape_only_s":       0.0,
        "t_total_s":             0.0,
        "success":               False,
        "reason":                "",
        "non_empty_fields":      0,
    }

    match_start = time.perf_counter()

    # ── 0. URL sanity check ───────────────────────────────────────────────────
    if not _is_valid_soccerway_url(base_url):
        print(f"  ✗ Invalid URL (not a Soccerway match link): {base_url}")
        _early_exit(timing, "false", match_start)
        return {}, timing

    # Inject match_id from URL; season is resolved after summary
    data["match_id"] = extract_match_id(base_url)
    # data["season"]   = extract_season(base_url)

    # ── 1. Summary ────────────────────────────────────────────────────────────
    print("[1/3] Loading summary page...")
    t0 = time.perf_counter()
    summary_html = get_page_html(driver, base_url, extra_wait=3)
    t_summary    = round(time.perf_counter() - t0, 2)

    # Detect HTTP error pages
    page_text = summary_html.lower()
    title     = driver.title.lower()

    if "404" in title or "not found" in page_text:
        print("  ✗ 404 – page not found")
        _early_exit(timing, "404", match_start, t_summary=t_summary)
        return {}, timing

    if "403" in title or "forbidden" in page_text:
        print("  ✗ 403 – access forbidden")
        _early_exit(timing, "403", match_start, t_summary=t_summary)
        return {}, timing

    timing["t_summary_s"] = t_summary
    print(f"      → {timing['t_summary_s']}s")

    if debug:
        with open("debug_summary.html", "w", encoding="utf-8") as f:
            f.write(summary_html)
        print("      → Saved debug_summary.html")

    data.update(parse_summary(summary_html))

    # Detect broken / empty match (page loaded but no recognisable content)
    if not data.get("home_team") or not data.get("away_team"):
        print("  ✗ no_data – page loaded but team names missing")
        timing["t_summary_s"]     = t_summary
        timing["t_scrape_only_s"] = t_summary
        timing["t_total_s"]       = round(time.perf_counter() - match_start, 2)
        timing["success"]         = False
        timing["reason"]          = "no_data"
        timing["non_empty_fields"] = sum(1 for v in data.values() if v not in ("", None))
        return data, timing

    # # Refine season: if URL didn't have an explicit year range, use the match date
    # if not re.search(r"\d{4}-\d{4}", base_url):
    #     data["season"] = extract_season(base_url, match_date=data.get("date", ""))

    # Random sleep between summary → stats
    sleep1 = jitter_sleep(1.0, 3.0)
    timing["sleep_after_summary_s"] = round(sleep1, 2)
    print(f"      → slept {timing['sleep_after_summary_s']}s before stats")

    # ── 2. Stats ──────────────────────────────────────────────────────────────
    print("[2/3] Loading stats page...")
    t0         = time.perf_counter()
    stats_html = get_page_html(driver, stats_url, extra_wait=3)
    t_stats    = round(time.perf_counter() - t0, 2)
    timing["t_stats_s"] = t_stats
    print(f"      → {t_stats}s")

    if debug:
        with open("debug_stats.html", "w", encoding="utf-8") as f:
            f.write(stats_html)
        print("      → Saved debug_stats.html")

    stats_data = parse_stats(stats_html)
    data.update(stats_data)

    # Detect missing stats — ignore the card defaults ("0") when deciding
    has_stats = any(
        v not in ("", None, "0")
        for k, v in stats_data.items()
        if k not in ("home_yellow_cards", "away_yellow_cards",
                     "home_red_cards",    "away_red_cards")
    )

    if not has_stats:
        print("  ✗ no_stats – stats tab returned no data")
        timing["success"]         = False
        timing["reason"]          = "no_stats"
        timing["t_scrape_only_s"] = round(t_summary + t_stats, 2)
        timing["t_total_s"]       = round(time.perf_counter() - match_start, 2)
        timing["non_empty_fields"] = sum(1 for v in data.values() if v not in ("", None))
        return data, timing

    # Random sleep between stats → lineups
    sleep2 = jitter_sleep(1.0, 3.0)
    timing["sleep_after_stats_s"] = round(sleep2, 2)
    print(f"      → slept {timing['sleep_after_stats_s']}s before lineups")

    # ── 3. Lineups ────────────────────────────────────────────────────────────
    print("[3/3] Loading lineups page...")
    t0           = time.perf_counter()
    lineups_html = get_page_html(driver, lineups_url, extra_wait=3)
    t_lineups    = round(time.perf_counter() - t0, 2)
    timing["t_lineups_s"] = t_lineups
    print(f"      → {t_lineups}s")

    if debug:
        with open("debug_lineups.html", "w", encoding="utf-8") as f:
            f.write(lineups_html)
        print("      → Saved debug_lineups.html")

    lineups_data = parse_lineups(lineups_html)
    data.update(lineups_data)

    # Detect missing lineups
    has_lineups = any(
        v not in ("", None)
        for v in lineups_data.values()
    )
    if not has_lineups:
        print("  ✗ no_lineups – lineups tab returned no formation/rating data")
        timing["success"]          = False
        timing["reason"]           = "no_lineups"
        timing["t_scrape_only_s"]  = round(t_summary + t_stats + t_lineups, 2)
        timing["t_total_s"]        = round(time.perf_counter() - match_start, 2)
        timing["non_empty_fields"] = sum(1 for v in data.values() if v not in ("", None))
        return data, timing

    # ── All good ──────────────────────────────────────────────────────────────
    timing["t_scrape_only_s"]  = round(t_summary + t_stats + t_lineups, 2)
    timing["t_total_s"]        = round(time.perf_counter() - match_start, 2)
    timing["success"]          = True
    timing["reason"]           = ""           # None written to log on success
    timing["non_empty_fields"] = sum(1 for v in data.values() if v not in ("", None))

    return data, timing


# ─── Column order & CSV output ────────────────────────────────────────────────

COLUMN_ORDER = [
    # ── Identity ──────────────────────────────────────────────────────────────
    "match_id",
    "league_division",
    # "season",
    "round",
    "date",
    "kickoff_time",
    "attendance",
    "capacity",
    "stadium",
    "city",
    "home_team",
    "away_team",
    # ── Result ────────────────────────────────────────────────────────────────
    "home_goals",
    "away_goals",
    "result",
    # ── Tactical ──────────────────────────────────────────────────────────────
    "home_formation",
    "away_formation",
    "home_team_rating",
    "away_team_rating",
    # ── Shots ─────────────────────────────────────────────────────────────────
    "home_shots_total",           "away_shots_total",
    "home_shots_on_target",       "away_shots_on_target",
    "home_shots_off_target",      "away_shots_off_target",
    "home_shots_inside_box",      "away_shots_inside_box",
    "home_shots_outside_box",     "away_shots_outside_box",
    "home_headed_goals",          "away_headed_goals",
    "home_hit_woodwork",          "away_hit_woodwork",
    # ── Set pieces ────────────────────────────────────────────────────────────
    "home_corners",               "away_corners",
    "home_free_kicks",            "away_free_kicks",
    "home_throw_ins",             "away_throw_ins",
    # ── Discipline ────────────────────────────────────────────────────────────
    "home_fouls_committed",       "away_fouls_committed",
    "home_offsides",              "away_offsides",
    "home_yellow_cards",          "away_yellow_cards",
    "home_red_cards",             "away_red_cards",
    # ── Goalkeeper ────────────────────────────────────────────────────────────
    "home_goalkeeper_saves",      "away_goalkeeper_saves",
    # ── Possession / Pressure ─────────────────────────────────────────────────
    "home_possession_pct",        "away_possession_pct",
    "home_touches_opp_box",       "away_touches_opp_box",
    # home_attacks / away_attacks removed — not served by Soccerway
    # ── Passing (pct + counts) ────────────────────────────────────────────────
    "home_passes_pct",            "away_passes_pct",
    "home_passes_successful",     "away_passes_successful",
    "home_passes_total",          "away_passes_total",

    "home_long_passes_pct",       "away_long_passes_pct",
    "home_long_passes_successful","away_long_passes_successful",
    "home_long_passes_total",     "away_long_passes_total",

    "home_passes_final_third_pct",     "away_passes_final_third_pct",
    "home_passes_final_third_successful", "away_passes_final_third_successful",
    "home_passes_final_third_total",   "away_passes_final_third_total",
    
    "home_crosses_pct",           "away_crosses_pct",
    "home_crosses_successful",    "away_crosses_successful",
    "home_crosses_total",         "away_crosses_total",

    # ── Other ─────────────────────────────────────────────────────────────
    "home_big_chances",           "away_big_chances",
    "home_duels_won",             "away_duels_won",
    "home_errors_leading_to_shot","away_errors_leading_to_shot",
    "home_errors_leading_to_goal","away_errors_leading_to_goal",
    "home_accurate_through_passes","away_accurate_through_passes",
    # ── Defending ─────────────────────────────────────────────────────────────
    "home_tackles_pct",           "away_tackles_pct",
    "home_tackles_successful",    "away_tackles_successful",
    "home_tackles_total",         "away_tackles_total",
    "home_shots_blocked",         "away_shots_blocked",
    "home_clearances",            "away_clearances",
    "home_interceptions",         "away_interceptions",
    # ── Expected stats ────────────────────────────────────────────────────────
    "home_xg",                    "away_xg",
    "home_xgot",                  "away_xgot",
    "home_xa",                    "away_xa",
    "home_xgot_faced",            "away_xgot_faced",
    "home_goals_prevented",       "away_goals_prevented",
]


# ─── Log CSV helpers ──────────────────────────────────────────────────────────

LOG_COLUMNS = [
    "scraped_at",
    "url",
    "success",
    "reason",
    "non_empty_fields",
    "t_summary_s",
    "sleep_after_summary_s",
    "t_stats_s",
    "sleep_after_stats_s",
    "t_lineups_s",
    "t_scrape_only_s",
    "t_total_s",
    "sleep_before_next_match_s",
    "cumulative_total_s",
]


def append_log(log_path: str, row: dict) -> None:
    """Append one timing row to the log CSV, creating headers if file is new."""
    file_exists = os.path.isfile(log_path)
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_COLUMNS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ─── CLI ──────────────────────────────────────────────────────────────────────

DEFAULT_URL = "https://us.soccerway.com/game/bournemouth-OtpNdwrc/manchester-united-ppjDR086/?mid=QZ5U62OH"

def derive_names_from_file(file_path: str) -> tuple[str, str]:
    base = os.path.splitext(os.path.basename(file_path))[0]
    output = f"{base}_match_data.csv"
    log = f"{base}_scrape_log.csv"
    return output, log

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Soccerway match pages (summary + stats + lineups) to CSV"
    )
    parser.add_argument("--url",    help="Single Soccerway match URL")
    parser.add_argument("--file",   help="Text file with multiple URLs (one per line)")
    parser.add_argument("--output", default="match_data.csv",  help="Output data CSV")
    parser.add_argument("--log",    default="scrape_log.csv",  help="Log CSV file (appended)")
    parser.add_argument("--debug",  action="store_true",
                        help="Save raw HTML files for inspection (debug_*.html)")
    args = parser.parse_args()

    # Auto-name output/log if --file is used and user didn't override
    if args.file:
        auto_output, auto_log = derive_names_from_file(args.file)

        if args.output == "match_data.csv":
            args.output = auto_output

        if args.log == "scrape_log.csv":
            args.log = auto_log

    # ── Build URL list ────────────────────────────────────────────────────────
    urls = []
    if args.file:
        with open(args.file, "r") as f:
            urls = [line.strip() for line in f if line.strip()]
    elif args.url:
        urls = [args.url]
    else:
        urls = [DEFAULT_URL]

    total = len(urls)
    print(f"\n{'='*60}")
    print(f"  Scraping {total} match(es)")
    print(f"  Output  : {args.output}")
    print(f"  Log     : {args.log}")
    print(f"{'='*60}")

    # ── Shared driver (opened once for all matches) ───────────────────────────
    driver = build_driver()

    all_data         = []
    cumulative_total = 0.0
    run_start        = time.perf_counter()

    try:
        for i, url in enumerate(urls, start=1):
            print(f"\n{'─'*60}")
            print(f"  Match {i}/{total}")
            print(f"{'─'*60}")

            log_row = {col: "" for col in LOG_COLUMNS}
            log_row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_row["url"]        = url
            log_row["success"]    = False

            try:
                data, timing = scrape_match(url, driver=driver, debug=args.debug)
                all_data.append(data)

                # ── Print match summary ───────────────────────────────────────
                print(f"\n── Match data (non-empty fields) {'─'*28}")
                for col in COLUMN_ORDER:
                    v = data.get(col, "")
                    if v not in ("", None):
                        print(f"  {col:<40}: {v}")
                print("─" * 60)

                # ── Save data CSV incrementally ───────────────────────────────
                df = pd.DataFrame(all_data).reindex(columns=COLUMN_ORDER)
                df.to_csv(args.output, index=False)

                # ── Timing summary for this match ─────────────────────────────
                cumulative_total += timing["t_total_s"]
                log_row.update({
                    "success":              timing.get("success", False),
                    "reason":               timing.get("reason") or "none",
                    "non_empty_fields":     timing["non_empty_fields"],
                    "t_summary_s":          timing["t_summary_s"],
                    "sleep_after_summary_s":timing["sleep_after_summary_s"],
                    "t_stats_s":            timing["t_stats_s"],
                    "sleep_after_stats_s":  timing["sleep_after_stats_s"],
                    "t_lineups_s":          timing["t_lineups_s"],
                    "t_scrape_only_s":      timing["t_scrape_only_s"],
                    "t_total_s":            timing["t_total_s"],
                    "cumulative_total_s":   round(cumulative_total, 2),
                })

                print(f"\n  ⏱  Summary  : {timing['t_summary_s']}s  "
                      f"(+{timing['sleep_after_summary_s']}s sleep)")
                print(f"  ⏱  Stats    : {timing['t_stats_s']}s  "
                      f"(+{timing['sleep_after_stats_s']}s sleep)")
                print(f"  ⏱  Lineups  : {timing['t_lineups_s']}s")
                print(f"  ⏱  Pure scrape (no sleep) : {timing['t_scrape_only_s']}s")
                print(f"  ⏱  Match total     : {timing['t_total_s']}s")
                print(f"  ⏱  Cumulative total: {cumulative_total:.2f}s  "
                      f"({cumulative_total/60:.1f} min)")

            except Exception as e:
                print(f"\n  ✗ ERROR scraping {url}: {e}")
                log_row["success"] = False
                log_row["reason"]  = "else"
                cumulative_total += 0
                log_row["cumulative_total_s"] = round(cumulative_total, 2)

            # ── Random sleep between matches (skip after last match) ───────────
            sleep_between = 0.0
            if i < total:
                sleep_between = jitter_sleep(8.0, 15.0)
                print(f"  💤 Sleeping {sleep_between:.1f}s before next match...")

            log_row["sleep_before_next_match_s"] = round(sleep_between, 2)
            cumulative_total += sleep_between
            log_row["cumulative_total_s"] = round(cumulative_total, 2)

            append_log(args.log, log_row)

    finally:
        driver.quit()
        print(f"\n  Chrome closed.")

    # ── Final summary ─────────────────────────────────────────────────────────
    wall_time = time.perf_counter() - run_start
    success_count = sum(1 for r in all_data if r)
    print(f"\n{'='*60}")
    print(f"  Done — {success_count}/{total} matches scraped successfully")
    print(f"  Wall time     : {wall_time:.1f}s  ({wall_time/60:.1f} min)")
    print(f"  Data saved to : {args.output}")
    print(f"  Log saved to  : {args.log}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
