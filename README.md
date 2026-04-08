# Football Data: Scraping, Analysis, and Prediction

An end-to-end machine learning project covering the collection, analysis, and prediction of football match data across multiple leagues and seasons.

**Dataset:** [kaggle.com/omarameen99](https://www.kaggle.com/omarameen99)

---

## Table of Contents

- [Project Structure](#project-structure)
- [1. Web Scraping](#1-web-scraping)
  - [Requirements](#requirements)
  - [Link Scraper](#link-scraper)
  - [Match Scraper](#match-scraper)
- [2. Analysis](#2-analysis)
- [3. Machine Learning](#3-machine-learning)

---

## Project Structure

```
project/
├── link_scraper.py          # Stage 1: collect match URLs
├── match_scraper.py         # Stage 2: scrape match data
│
├── links/                   # .txt files produced by link_scraper.py
├── scraped_links/           # .txt files moved here after scraping
├── scraped_data/            # match data CSVs
└── scraped_logs/            # per-file scrape log CSVs
```

---

## 1. Web Scraping

**Source:** [us.soccerway.com](https://us.soccerway.com)

Scraping is split into two stages. The link scraper collects all match page URLs for a given league and season and stores them in plain text files. The match scraper then reads those files and extracts detailed match data from each URL, saving the results to CSV.

Both scripts share a set of design principles to reduce the risk of being blocked:

- A single Chrome driver instance is opened once and reused for the entire run, avoiding the overhead and fingerprinting risk of repeated browser launches.
- Randomised sleep intervals are applied between page loads, button clicks, and consecutive matches to simulate human browsing behaviour.
- The match scraper applies an additional random delay of 8–15 seconds between consecutive matches.

---

### Requirements

```bash
pip install selenium webdriver-manager pandas beautifulsoup4
```

Google Chrome must be installed on the system. `webdriver-manager` downloads the matching ChromeDriver automatically.

---

### Link Scraper

**Script:** `link_scraper.py`

Navigates to a league's results page, clicks "Show more matches" repeatedly until all matches are loaded, then extracts and saves every match URL. Only links from within match-row containers are collected; navigation links, team profile pages, and other incidental links on the page are excluded.

Output files are named after the league and season they correspond to, for example:

```
egypt_premier_league_2024-2025.txt
england_premier_league_2025-2026.txt
england_premier_league_2024-2025.txt
germany_bundesliga_2024-2025.txt
```

The `egypt_` prefix is used to distinguish the Egyptian Premier League from the English one. All 28 configured leagues follow the same naming convention.

**Modes and flags:**

| Flag | Description |
|---|---|
| `--link URL` | Scrape a single results page URL |
| `--file FILE` | Read one results-page URL per line from a `.txt` file and scrape each one |
| `--league KEY` | Scrape all configured seasons for the league whose key contains `KEY` (substring match) |
| `--prefix PREFIX` | Scrape all leagues whose filename prefix contains `PREFIX` (e.g. `germany_`) |
| `--output-dir DIR` | Directory to save output `.txt` files (default: `./links`) |
| *(no flags)* | Scrape all 28 configured leagues (current season + 2024–2025) |

**Examples:**

```bash
# Test a single results page
python link_scraper.py --link "https://us.soccerway.com/germany/bundesliga/results/"

# Scrape from a custom list of results URLs
python link_scraper.py --file my_urls.txt --output-dir ./out

# Scrape one configured league (both seasons)
python link_scraper.py --league bundesliga

# Scrape all German leagues
python link_scraper.py --prefix germany_

# Scrape all configured leagues
python link_scraper.py --output-dir ./links
```

---

### Match Scraper

**Script:** `match_scraper.py`

For each match URL, the scraper visits three tabs on the match page — Summary, Stats, and Lineups — and consolidates the data into a single row. Results are written to a CSV file incrementally after each match so that progress is not lost if the run is interrupted. A separate log CSV is written alongside the data file to record timing information, success status, and failure reasons for every URL.

**Data collected:**

- **Summary:** league, division, season, round, date, kick-off time, home team, away team, score, result, attendance, capacity, stadium, city
- **Stats:** shots on/off target, possession, corners, fouls, yellow/red cards, offsides, passes, crosses, tackles, clearances, interceptions, xG, xA, and more
- **Lineups:** home and away formation, home and away team rating

**File management:** after a `.txt` file has been fully processed, the scraper moves it from `links/` to `scraped_links/`. Data CSVs are saved to `scraped_data/` and log CSVs to `scraped_logs/`, each named after the source `.txt` file.

**Log columns:**

`scraped_at`, `url`, `success`, `reason`, `non_empty_fields`, `t_summary_s`, `sleep_after_summary_s`, `t_stats_s`, `sleep_after_stats_s`, `t_lineups_s`, `t_scrape_only_s`, `t_total_s`, `sleep_before_next_match_s`, `cumulative_total_s`

The `reason` field records why a match was not scraped successfully, for example `no_data`, `403`, `404`, or a truncated exception message.

**Flags:**

| Flag | Description |
|---|---|
| `--url URL` | Scrape a single match page URL |
| `--file FILE [FILE ...]` | One or more `.txt` files of match URLs; each file gets its own data and log CSV |
| `--dir DIR` | Directory to scan for `.txt` files; every `.txt` found is processed |
| `--output FILE` | Output data CSV name (only used with `--url`; default: `match_data.csv`) |
| `--log FILE` | Log CSV name (only used with `--url`; default: `scrape_log.csv`) |
| `--debug` | Save raw HTML to `debug_*.html` files for inspection |

**Examples:**

```bash
# Scrape a single match (useful for testing)
python match_scraper.py --url "https://us.soccerway.com/match/..."

# Scrape all matches in one file
python match_scraper.py --file links/germany_bundesliga_2024-2025.txt

# Scrape multiple files in one run
python match_scraper.py --file links/england_premier_league_2024-2025.txt \
                                   links/england_championship_2024-2025.txt

# Scrape every .txt file in a directory
python match_scraper.py --dir ./links

# Combine --dir with extra files
python match_scraper.py --dir ./links --file extra_matches.txt

# Save a single match to a named file
python match_scraper.py --url "https://us.soccerway.com/match/..." \
                           --output bvb_vs_fcb.csv --log bvb_vs_fcb_log.csv

# Debug mode
python match_scraper.py --file links/test.txt --debug
```

---

## 2. Analysis

eda

---

## 3. Machine Learning

model