"""
scrape_links.py
---------------
Scrapes Soccerway match result URLs across multiple leagues / seasons.

REQUIREMENTS:
    pip install selenium webdriver-manager beautifulsoup4

── MODES ─────────────────────────────────────────────────────────────────────
  --link URL          Scrape a single results page, save to --output-dir
  --file FILE         Read one results-page URL per line, scrape each one
  --league KEY        Scrape all configured seasons for one league (substring)
  --prefix PREFIX     Scrape all leagues whose prefix matches (e.g. egypt_)
  (no args)           Scrape ALL configured leagues (current + 2024-2025)

── EXAMPLES ──────────────────────────────────────────────────────────────────
  python scrape_links.py --link "https://us.soccerway.com/germany/bundesliga/results/"
  python scrape_links.py --file my_urls.txt --output-dir ./out
  python scrape_links.py --league bundesliga
  python scrape_links.py --prefix germany_
  python scrape_links.py --output-dir ./links
"""

import argparse
import os
import re
import sys
import time
import random
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager


# ══════════════════════════════════════════════════════════════════════════════
# LEAGUE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════
# Keys:
#   key         -> used in output filename
#   prefix      -> prepended to filename (e.g. "egypt_" to avoid name clashes)
#   archive_url -> /archive/ listing page; current season = replace with /results/
#   base_slug   -> slug used to build older-season URLs
#   results_url -> only for single-season leagues (no prev season scraped)

LEAGUES = [
    # Egypt
    dict(key="premier_league",       prefix="egypt_",         archive_url="https://us.soccerway.com/egypt/premier-league/archive/",                   base_slug="premier-league"),
    dict(key="egypt_cup",            prefix="egypt_",         archive_url="https://us.soccerway.com/egypt/egypt-cup/archive/",                        base_slug="egypt-cup"),
    # Europe leagues
    dict(key="premier_league",       prefix="england_",       archive_url="https://us.soccerway.com/england/premier-league/archive/",                base_slug="premier-league"),
    dict(key="ligue_1",              prefix="france_",        archive_url="https://us.soccerway.com/france/ligue-1/archive/",                         base_slug="ligue-1"),
    dict(key="bundesliga",           prefix="germany_",       archive_url="https://us.soccerway.com/germany/bundesliga/archive/",                     base_slug="bundesliga"),
    dict(key="serie_a",              prefix="italy_",         archive_url="https://us.soccerway.com/italy/serie-a/archive/",                          base_slug="serie-a"),
    dict(key="laliga",               prefix="spain_",         archive_url="https://us.soccerway.com/spain/laliga/archive/",                           base_slug="laliga"),
    dict(key="eredivisie",           prefix="netherlands_",   archive_url="https://us.soccerway.com/netherlands/eredivisie/archive/",                 base_slug="eredivisie"),
    # Second divisions
    dict(key="championship",         prefix="england_",       archive_url="https://us.soccerway.com/england/championship/archive/",                   base_slug="championship"),
    dict(key="ligue_2",              prefix="france_",        archive_url="https://us.soccerway.com/france/ligue-2/archive/",                         base_slug="ligue-2"),
    dict(key="2_bundesliga",         prefix="germany_",       archive_url="https://us.soccerway.com/germany/2-bundesliga/archive/",                   base_slug="2-bundesliga"),
    dict(key="serie_b",              prefix="italy_",         archive_url="https://us.soccerway.com/italy/serie-b/archive/",                          base_slug="serie-b"),
    dict(key="laliga2",              prefix="spain_",         archive_url="https://us.soccerway.com/spain/laliga2/archive/",                          base_slug="laliga2"),
    dict(key="eerste_divisie",       prefix="netherlands_",   archive_url="https://us.soccerway.com/netherlands/eerste-divisie/archive/",             base_slug="eerste-divisie"),
    # Europe other
    dict(key="champions_league",     prefix="europe_",        archive_url="https://us.soccerway.com/europe/champions-league/archive/",                base_slug="champions-league"),
    dict(key="europa_league",        prefix="europe_",        archive_url="https://us.soccerway.com/europe/europa-league/archive/",                   base_slug="europa-league"),
    dict(key="conference_league",    prefix="europe_",        archive_url="https://us.soccerway.com/europe/conference-league/archive/",               base_slug="conference-league"),
    dict(key="nations_league",       prefix="europe_",        archive_url="https://us.soccerway.com/europe/uefa-nations-league/archive/",             base_slug="uefa-nations-league"),
    # World
    dict(key="world_cup",            prefix="world_",         results_url="https://us.soccerway.com/world/world-cup/results/",                        base_slug="world-cup"),
    dict(key="club_world_cup",       prefix="world_",         archive_url="https://us.soccerway.com/world/fifa-club-world-cup/archive/",              base_slug="fifa-club-world-cup"),
    # South America
    dict(key="copa_libertadores",    prefix="south_america_", archive_url="https://us.soccerway.com/south-america/copa-libertadores/archive/",        base_slug="copa-libertadores"),
    dict(key="copa_sudamericana",    prefix="south_america_", archive_url="https://us.soccerway.com/south-america/copa-sudamericana/archive/",        base_slug="copa-sudamericana"),
    dict(key="liga_mx",              prefix="mexico_",        archive_url="https://us.soccerway.com/mexico/liga-mx/archive/",                         base_slug="liga-mx"),
    # Japan
    dict(key="j1_league",            prefix="japan_",         archive_url="https://us.soccerway.com/japan/j1-league/archive/",                        base_slug="j1-league"),
    dict(key="j2_league",            prefix="japan_",         archive_url="https://us.soccerway.com/japan/j2-league/archive/",                        base_slug="j2-league"),
    # Middle East
    dict(key="saudi_pro_league",     prefix="saudi_",         archive_url="https://us.soccerway.com/saudi-arabia/saudi-professional-league/archive/", base_slug="saudi-professional-league"),
    dict(key="super_lig",            prefix="turkey_",        archive_url="https://us.soccerway.com/turkey/super-lig/archive/",                       base_slug="super-lig"),
    # Africa
    dict(key="caf_champions_league", prefix="africa_",        archive_url="https://us.soccerway.com/africa/caf-champions-league/archive/",            base_slug="caf-champions-league"),
]

PREV_SEASON = "2024-2025"

# Strict match-href pattern: /match/<home-slug>/<away-slug>/?mid=<id>
MATCH_HREF_RE = re.compile(
    r"^https?://[^/]+/game/[^/]+/[^/]+/\?mid=[A-Za-z0-9]+$"
)


# ══════════════════════════════════════════════════════════════════════════════
# TERMINAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

WIDTH = 66

def hr(char="─"):
    return char * WIDTH

def banner(text, char="═"):
    print(f"\n{char * WIDTH}")
    print(f"  {text}")
    print(f"{char * WIDTH}")

def info(text):   print(f"  {text}")
def ok(text):     print(f"  \u2713 {text}")
def warn(text):   print(f"  \u26a0  {text}")
def err(text):    print(f"  \u2717 {text}")

def fmt_time(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"


# ══════════════════════════════════════════════════════════════════════════════
# SLEEP HELPERS  (human-like delays to avoid getting blocked)
# ══════════════════════════════════════════════════════════════════════════════

def sleep_page_load():
    """After navigating to a new page — wait for JS to settle."""
    time.sleep(random.uniform(3.0, 5.5))

def sleep_after_click():
    """After clicking 'Show more' — wait for new rows to render."""
    time.sleep(random.uniform(2.0, 4.0))

def sleep_between_seasons():
    t = random.uniform(5.0, 9.0)
    info(f"Sleeping {t:.1f}s between seasons …")
    time.sleep(t)

def sleep_between_leagues():
    t = random.uniform(8.0, 15.0)
    info(f"Sleeping {t:.1f}s between leagues …")
    time.sleep(t)

def sleep_between_urls():
    t = random.uniform(6.0, 11.0)
    info(f"Sleeping {t:.1f}s before next URL …")
    time.sleep(t)


# ══════════════════════════════════════════════════════════════════════════════
# DRIVER  (opened once, reused for everything)
# ══════════════════════════════════════════════════════════════════════════════

def build_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    drv = webdriver.Chrome(service=service, options=opts)
    # Hide the webdriver flag
    drv.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
    )
    return drv


# ══════════════════════════════════════════════════════════════════════════════
# MATCH LINK EXTRACTION  (strict — only real match rows)
# ══════════════════════════════════════════════════════════════════════════════

def extract_match_links_from_html(html):
    """
    Extract match URLs only from inside event__match row divs.
    This is the same container visible in the pasted HTML:
      <div class="event__match event__match--withRowLink ...">
        <a href="/match/team1-.../team2-.../?mid=XXX" class="eventRowLink">
    Two-layer filter:
      1. Parent div must have 'event__match' in its class.
      2. href must match the strict MATCH_HREF_RE pattern.
    Returns a deduplicated, sorted list.
    """
    soup  = BeautifulSoup(html, "html.parser")
    links = set()

    for row in soup.find_all("div", class_=lambda c: c and "event__match" in c):
        for a in row.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            # Make absolute
            if href.startswith("/"):
                href = "https://us.soccerway.com" + href
            # Normalise domain (www -> us)
            href = re.sub(
                r"^https?://(?:www\.)?soccerway\.com",
                "https://us.soccerway.com",
                href,
            )
            if MATCH_HREF_RE.match(href):
                links.add(href)

    return sorted(links)


# ══════════════════════════════════════════════════════════════════════════════
# SHOW MORE + FULL PAGE SCRAPE
# ══════════════════════════════════════════════════════════════════════════════

def scrape_results_page(driver, url, label=""):
    """
    Load a /results/ page, click 'Show more matches' until it disappears,
    extract all match links, and return them.
    Prints live progress to the terminal.
    """
    tag = label or url
    info(f"Loading: {url}")
    driver.get(url)
    sleep_page_load()

    clicks     = 0
    prev_count = -1

    while True:
        # Refresh count for live display
        current_links = extract_match_links_from_html(driver.page_source)
        n = len(current_links)
        if n != prev_count:
            prev_count = n
            print(f"\r  {tag} — {n} match links collected …", end="", flush=True)

        btn = _find_show_more_button(driver)
        if btn is None:
            break  # No more button = all loaded

        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.5)
            btn.click()
            clicks += 1
            print(f"\r  {tag} — {n} links | clicked 'Show more' ({clicks}x) …", end="", flush=True)
            sleep_after_click()
        except Exception as exc:
            warn(f"\nShow-more click #{clicks + 1} failed: {exc}")
            break

    # Final extraction after all content loaded
    links = extract_match_links_from_html(driver.page_source)
    print()  # end the \r line
    ok(f"{tag} — {len(links)} match links  ({clicks} 'Show more' click(s))")
    return links


def _find_show_more_button(driver):
    """Return the 'Show more matches' button element or None."""
    # Primary selector (from the provided HTML)
    for el in driver.find_elements(By.CSS_SELECTOR, "button[data-testid='wcl-buttonLink']"):
        try:
            if "show more" in el.text.strip().lower():
                return el
        except Exception:
            pass
    # Fallback: any button
    for el in driver.find_elements(By.TAG_NAME, "button"):
        try:
            if "show more" in el.text.strip().lower():
                return el
        except Exception:
            pass
    # Fallback 2: span inside button
    for el in driver.find_elements(By.CSS_SELECTOR, "button span"):
        try:
            if "show more" in el.text.strip().lower():
                return el.find_element(By.XPATH, "..")
        except Exception:
            pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# ARCHIVE / SEASON URL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def detect_current_season_label(driver, archive_url):
    """
    Visit the archive page and return the label of the most recent season
    (e.g. '2025-2026').  Falls back to a calendar heuristic.
    """
    try:
        info(f"Detecting current season from archive …")
        driver.get(archive_url)
        sleep_page_load()
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            if re.match(r"\d{4}[-/]\d{4}", text):
                label = re.sub(r"/", "-", text.strip())
                info(f"Detected current season: {label}")
                return label
    except Exception as exc:
        warn(f"Archive detection failed ({exc}), using heuristic.")

    today = datetime.now()
    y = today.year
    label = f"{y}-{y + 1}" if today.month >= 7 else f"{y - 1}-{y}"
    info(f"Heuristic season: {label}")
    return label


def get_prev_season_url(driver, archive_url, season, base_slug):
    """
    Find the /results/ URL for a specific older season.
    1. Scan the archive page for a matching link.
    2. Fall back to constructing the URL.
    """
    try:
        driver.get(archive_url)
        sleep_page_load()
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a["href"]
            if season in text or season in href:
                full = href if href.startswith("http") else "https://us.soccerway.com" + href
                if "/results" not in full:
                    full = full.rstrip("/") + "/results/"
                info(f"Found {season} in archive: {full}")
                return full
    except Exception as exc:
        warn(f"Archive scan failed ({exc}), constructing URL.")

    base = archive_url.replace("/archive/", "")
    constructed = f"{base}-{season}/results/"
    warn(f"Constructed URL for {season}: {constructed}")
    return constructed


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def save_links(links, output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        for url in links:
            f.write(url + "\n")
    return path


def make_filename(prefix, key, season):
    return f"{prefix}{key}_{season}.txt"


def slug_from_url(url):
    """Build a safe filename slug from a URL."""
    parts = [p for p in url.rstrip("/").split("/") if p and p not in ("https:", "us.soccerway.com")]
    slug  = "_".join(parts[-3:]) if len(parts) >= 3 else "_".join(parts)
    return re.sub(r"[^a-z0-9_-]", "_", slug.lower())


# ══════════════════════════════════════════════════════════════════════════════
# LEAGUE PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

def process_league(driver, league, output_dir):
    prefix       = league.get("prefix", "")
    key          = league["key"]
    base_slug    = league["base_slug"]
    results_only = "results_url" in league

    banner(f"League: {prefix}{key}")

    # Single-season mode (e.g. World Cup)
    if results_only:
        results_url  = league["results_url"]
        season_label = "current"
        try:
            driver.get(results_url)
            sleep_page_load()
            soup = BeautifulSoup(driver.page_source, "html.parser")
            for node in soup.find_all(string=re.compile(r"\d{4}[-/]\d{4}")):
                m = re.search(r"\d{4}[-/]\d{4}", node)
                if m:
                    season_label = m.group(0).replace("/", "-")
                    break
        except Exception:
            pass

        tag   = f"{prefix}{key} {season_label}"
        links = scrape_results_page(driver, results_url, tag)
        if links:
            fname = make_filename(prefix, key, season_label)
            path  = save_links(links, output_dir, fname)
            ok(f"Saved {len(links)} URLs → {path}")
        else:
            err(f"No match links found for {tag}")
        return

    # Archive-based: current + 2024-2025
    archive_url    = league["archive_url"]
    current_season = detect_current_season_label(driver, archive_url)

    seasons = []
    current_results = archive_url.replace("/archive/", "/results/")
    seasons.append((current_season, current_results))

    if current_season != PREV_SEASON:
        prev_url = get_prev_season_url(driver, archive_url, PREV_SEASON, base_slug)
        seasons.append((PREV_SEASON, prev_url))
    else:
        info(f"Current season IS {PREV_SEASON} — scraping once only.")

    for idx, (season_label, results_url) in enumerate(seasons):
        info(f"\n  -- {season_label}  ->  {results_url}")
        tag   = f"{prefix}{key} {season_label}"
        links = scrape_results_page(driver, results_url, tag)

        if links:
            fname = make_filename(prefix, key, season_label)
            path  = save_links(links, output_dir, fname)
            ok(f"Saved {len(links)} URLs -> {path}")
        else:
            err(f"No match links found for {tag}")

        if idx < len(seasons) - 1:
            sleep_between_seasons()


# ══════════════════════════════════════════════════════════════════════════════
# SINGLE LINK MODE
# ══════════════════════════════════════════════════════════════════════════════

def process_single_link(driver, url, output_dir):
    banner(f"Single link: {url}")
    links = scrape_results_page(driver, url, label=url)

    if links:
        fname = slug_from_url(url) + ".txt"
        path  = save_links(links, output_dir, fname)
        ok(f"Saved {len(links)} URLs -> {path}")
    else:
        err("No match links found.")

    return links


# ══════════════════════════════════════════════════════════════════════════════
# FILE MODE  (one results URL per line)
# ══════════════════════════════════════════════════════════════════════════════

def process_file(driver, file_path, output_dir):
    with open(file_path, "r", encoding="utf-8") as fh:
        urls = [ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")]

    if not urls:
        err(f"No URLs found in {file_path}")
        return

    banner(f"File: {file_path}  ({len(urls)} URLs)")

    for idx, url in enumerate(urls, start=1):
        info(f"\n[{idx}/{len(urls)}] {url}")
        tag   = f"URL {idx}/{len(urls)}"
        links = scrape_results_page(driver, url, tag)

        if links:
            fname = slug_from_url(url) + ".txt"
            path  = save_links(links, output_dir, fname)
            ok(f"Saved {len(links)} URLs -> {path}")
        else:
            err(f"No match links found for URL {idx}")

        if idx < len(urls):
            sleep_between_urls()


# ══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(output_dir, wall_time):
    print(f"\n{hr('=')}")
    print(f"  DONE  --  Total time: {fmt_time(wall_time)}")
    print(hr("="))

    if not os.path.isdir(output_dir):
        return

    txt_files = sorted(f for f in os.listdir(output_dir) if f.endswith(".txt"))
    if not txt_files:
        return

    print(f"\n  Output files in '{output_dir}':\n")
    total = 0
    for fname in txt_files:
        path  = os.path.join(output_dir, fname)
        count = sum(1 for _ in open(path, encoding="utf-8"))
        total += count
        print(f"    {fname:<65} {count:>5} URLs")

    print(f"\n    {'TOTAL':<65} {total:>5} URLs\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Soccerway match result URLs",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--link",         metavar="URL",    help="Scrape a single results page URL")
    parser.add_argument("--file",         metavar="FILE",   help="Text file with one results-page URL per line")
    parser.add_argument("--league", "-l", metavar="KEY",    help="Scrape league(s) whose key contains KEY")
    parser.add_argument("--prefix", "-p", metavar="PREFIX", help="Scrape league(s) whose prefix contains PREFIX")
    parser.add_argument("--output-dir", "-o", default="./links", metavar="DIR",
                        help="Folder to save .txt files (default: ./links)")
    args = parser.parse_args()

    modes = sum([bool(args.link), bool(args.file), bool(args.league), bool(args.prefix)])
    if modes > 1:
        parser.error("Use only ONE of --link, --file, --league, --prefix at a time.")

    start = time.perf_counter()

    # ── Build driver ONCE ─────────────────────────────────────────────────────
    banner("Starting Chrome (single instance for entire run)")
    driver = build_driver()
    ok("Chrome is ready.\n")

    try:
        if args.link:
            # ── --link ─────────────────────────────────────────────────────────
            process_single_link(driver, args.link.strip(), args.output_dir)

        elif args.file:
            # ── --file ─────────────────────────────────────────────────────────
            if not os.path.isfile(args.file):
                err(f"File not found: {args.file}")
                sys.exit(1)
            process_file(driver, args.file, args.output_dir)

        else:
            # ── --league / --prefix / all ──────────────────────────────────────
            leagues = LEAGUES
            if args.league:
                leagues = [l for l in leagues if args.league.lower() in l["key"].lower()]
            if args.prefix:
                leagues = [l for l in leagues if args.prefix.lower() in l.get("prefix", "").lower()]

            if not leagues:
                err("No leagues matched the filter.")
                sys.exit(1)

            info(f"Leagues queued : {len(leagues)}")
            info(f"Output dir     : {args.output_dir}\n")

            for idx, league in enumerate(leagues, start=1):
                label = f"{league.get('prefix','')}{league['key']}"
                info(f"\nLeague {idx}/{len(leagues)}: {label}")
                try:
                    process_league(driver, league, args.output_dir)
                except Exception as exc:
                    err(f"Error processing {label}: {exc}")

                if idx < len(leagues):
                    sleep_between_leagues()

    finally:
        driver.quit()
        wall = time.perf_counter() - start
        info("Chrome closed.")
        print_summary(args.output_dir, wall)


if __name__ == "__main__":
    main()
