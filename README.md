# Football Match Outcome Predictor

An end-to-end machine learning project covering the collection, processing, and prediction of football match outcomes across 55 leagues and competitions.

The project is structured in three stages: a multi-stage web scraper that collects detailed match data from us.soccerway.com, an exploratory analysis of the resulting dataset, and a machine learning pipeline that predicts match outcomes — Home Win, Draw, or Away Win — using only pre-match information derived from each team's recent form.

Rather than relying on in-match statistics such as shots or possession (which are unavailable before kickoff), the prediction model engineers rolling features from each team's last 5 matches and compares five classification models — Logistic Regression, Random Forest, XGBoost, LightGBM, and SVM — evaluated using per-class F1 score across all three outcome classes.

**Dataset:** [kaggle.com/omarameen99](https://www.kaggle.com/omarameen99)
---

## Table of Contents

- [Project Structure](#project-structure)
- [1. Web Scraping](#1-web-scraping)
  - [Requirements](#requirements)
  - [Link Scraper](#link-scraper)
  - [Match Scraper](#match-scraper)
  - [Full Scraper](#full-scraper)
  - [Retry Low Fields](#retry-low-fields)
  - [Combine CSVs](#combine-csvs)
- [2. Analysis](#2-analysis)
- [3. Machine Learning](#3-machine-learning)

---

## Project Structure

```
project/
├── scraper/
│   ├── link_scraper.py          # Stage 1: collect match URLs
│   ├── match_scraper.py         # Stage 2: scrape match data
│   ├── full_scraper.py          # Orchestrator: runs link_scraper → match_scraper
│   ├── retry_low_fields.py      # Re-scrape incomplete or failed rows
│   ├── combine_csv.py           # Merge multiple data CSVs into one
│   │
│   ├── links/                   # .txt files produced by link_scraper.py
│   ├── scraped_links/           # .txt files moved here after scraping
│   ├── scraped_data/            # match data CSVs
│   └── scraped_logs/            # per-file scrape log CSVs
│
└── link_scraping_log/           # text files tracking which leagues were scraped
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

**Script:** `scraper/link_scraper.py`

Navigates to a league's results page, clicks "Show more matches" repeatedly until all matches are loaded, then extracts and saves every match URL. Only links from within match-row containers are collected; navigation links, team profile pages, and other incidental links on the page are excluded.

Output files are named after the league and season they correspond to, for example:

```
egypt_premier_league_2024-2025.txt
```

The `egypt_` prefix is used to distinguish the Egyptian Premier League from the English one. All 28 configured leagues follow the same naming convention.

**Configured leagues:**

| Region | Leagues |
|---|---|
| Egypt | Premier League, Egypt Cup |
| England | Premier League, Championship |
| France | Ligue 1, Ligue 2 |
| Germany | Bundesliga, 2. Bundesliga |
| Italy | Serie A, Serie B |
| Spain | LaLiga, LaLiga 2 |
| Netherlands | Eredivisie, Eerste Divisie |
| Europe | Champions League, Europa League, Conference League, Nations League |
| World | World Cup, Club World Cup |
| South America | Copa Libertadores, Copa Sudamericana |
| Mexico | Liga MX |
| Japan | J1 League, J2 League |
| Middle East | Saudi Pro League, Süper Lig |
| Africa | CAF Champions League |

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
cd scraper

# Test a single results page
python link_scraper.py --link "https://us.soccerway.com/germany/bundesliga/results/"

# Scrape from a custom list of results URLs
python link_scraper.py --file my_urls.txt --output-dir ../links

# Scrape one configured league (both seasons)
python link_scraper.py --league bundesliga

# Scrape all German leagues
python link_scraper.py --prefix germany_

# Scrape all configured leagues
python link_scraper.py --output-dir ../links
```

---

### Match Scraper

**Script:** `scraper/match_scraper.py`

For each match URL, the scraper visits three tabs on the match page — Summary, Stats, and Lineups — and consolidates the data into a single row. Results are written to a CSV file incrementally after each match so that progress is not lost if the run is interrupted. A separate log CSV is written alongside the data file to record timing information, success status, and failure reasons for every URL.

**Data collected:**

- **Identity:** match ID, league/division, round, date, kick-off time, attendance, capacity, stadium, city, home team, away team, score, result
- **Tactical:** home/away formation, home/away team rating
- **Shots:** total, on/off target, inside/outside box, headed goals, hit woodwork
- **Set pieces:** corners, free kicks, throw-ins
- **Discipline:** fouls, offsides, yellow/red cards
- **Goalkeeper:** saves
- **Possession/Pressure:** possession %, touches in opposition box
- **Passing:** overall, long balls, final third passes (%, successful, total for each)
- **Crossing:** crosses (%, successful, total)
- **Other attacking:** big chances, duels won, errors leading to shot/goal, accurate through-passes
- **Defending:** tackles (%, successful, total), shots blocked, clearances, interceptions
- **Expected stats:** xG, xGOT, xA, xGOT faced, goals prevented

**File management:** after a `.txt` file has been fully processed, the scraper moves it from `links/` to `scraped_links/`. Data CSVs are saved to `scraped_data/` and log CSVs to `scraped_logs/`, each named after the source `.txt` file, for example:
```
egypt_premier_league_2024-2025_match_data.csv
egypt_premier_league_2024-2025_scrape_log.csv
```

**Log columns:**

`scraped_at`, `url`, `success`, `reason`, `non_empty_fields`, `t_summary_s`, `sleep_after_summary_s`, `t_stats_s`, `sleep_after_stats_s`, `t_lineups_s`, `t_scrape_only_s`, `t_total_s`, `sleep_before_next_match_s`, `cumulative_total_s`

The `reason` field records why a match was not scraped successfully, for example `no_data`, `403`, `404`, `no_stats`, `no_lineups`, or a truncated exception message.

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
cd scraper

# Scrape a single match (useful for testing)
python match_scraper.py --url "https://us.soccerway.com/game/..."

# Scrape all matches in one file
python match_scraper.py --file ../links/germany_bundesliga_2024-2025.txt

# Scrape multiple files in one run
python match_scraper.py --file ../links/england_premier_league_2024-2025.txt \
                                   ../links/england_championship_2024-2025.txt

# Scrape every .txt file in a directory
python match_scraper.py --dir ../links

# Save a single match to a named file
python match_scraper.py --url "https://us.soccerway.com/game/..." \
                           --output bvb_vs_fcb.csv --log bvb_vs_fcb_log.csv

# Debug mode
python match_scraper.py --file ../links/test.txt --debug
```

---

### Full Scraper

**Script:** `scraper/full_scraper.py`

Orchestrates the full pipeline by running `link_scraper.py` followed by `match_scraper.py` in a single command. Useful when you want to go directly from a results page URL (or a list of them) to finished match data CSVs without running each stage separately.

**Modes:**

- **URL mode** — takes a single results-page URL, runs `link_scraper.py --link` to produce a `.txt` file, then immediately runs `match_scraper.py --file` on it.
- **File mode** — takes a `.txt` file containing one results-page URL per line, runs `link_scraper.py --file` on it, then runs `match_scraper.py --dir` on the links directory.

**Flags:**

| Flag | Description |
|---|---|
| `URL` | Results-page URL to scrape (URL mode) |
| `--file FILE` / `-f FILE` | Text file with one results-page URL per line (file mode) |
| `--links-dir DIR` | Folder used for intermediate `.txt` link files (default: `./links`) |

**Examples:**

```bash
cd scraper

# URL mode: scrape one results page end-to-end
python full_scraper.py "https://us.soccerway.com/germany/bundesliga/results/"

# File mode: scrape all URLs listed in a file
python full_scraper.py --file ../my_results_pages.txt

# Custom links directory
python full_scraper.py "https://us.soccerway.com/spain/laliga/results/" --links-dir ../links
```

---

### Retry Low Fields

**Script:** `scraper/retry_low_fields.py`

After a scraping run, some matches may have succeeded but returned an unusually low number of populated fields (e.g. stats or lineups tabs failed to load). This script reads one or more `*_scrape_log.csv` files, identifies rows that need retrying, re-scrapes those URLs, and patches the data CSV in place if the new scrape yields more data.

A row is retried if any of the following apply:
- `non_empty_fields` is below the threshold (default: 30)
- `reason` is `no_stats` or `no_lineups` (regardless of field count)

A unified retry log (`scraping_failed_urls_log.csv`) is written to the script directory and appended to on every run.

**Retry log columns:**

All original log columns plus: `file_name`, `fields_before`, `fields_after`, `row_updated`, `retry_trigger`, `data_csv_found`

**Flags:**

| Flag | Description |
|---|---|
| `--log LOG_CSV [...]` | One or more `*_scrape_log.csv` files to process |
| `--dir DIR` | Directory to scan for `*_scrape_log.csv` files (existing `*_retry_log*.csv` files are skipped) |
| `--threshold N` | Re-scrape rows with `non_empty_fields` < N (default: 30). `no_stats` and `no_lineups` rows are always retried regardless |
| `--debug` | Save raw HTML debug files for each re-scrape |

**Examples:**

```bash
cd scraper

# Retry a single log file
python retry_low_fields.py --log ../scraped_logs/germany_bundesliga_2024-2025_scrape_log.csv

# Retry all log files in a directory
python retry_low_fields.py --dir ../scraped_logs

# Custom threshold
python retry_low_fields.py --dir ../scraped_logs --threshold 25

# Retry multiple specific log files
python retry_low_fields.py --log ../scraped_logs/england_premier_league_2024-2025_scrape_log.csv \
                                  ../scraped_logs/spain_laliga_2024-2025_scrape_log.csv
```

---

### Combine CSVs

**Script:** `scraper/combine_csv.py`

Concatenates all CSV files in a folder into a single output file. Optionally extracts the season from each filename and inserts it as a column.

**Flags:**

| Flag | Description |
|---|---|
| `--folder DIR` | Path to folder containing CSV files (required) |
| `--output FILE` | Output CSV file name (default: `combined.csv`) |
| `--season` | Add a `season` column extracted from each source filename |

**Examples:**

```bash
cd scraper

# Combine all CSVs in scraped_data into one file
python combine_csv.py --folder ../scraped_data --output ../merged.csv

# Same but add a season column derived from each filename
python combine_csv.py --folder ../scraped_data --output ../merged.csv --season
```

---

## 2. Analysis

eda

---

## 3. Machine Learning

model
