import os
import subprocess
import sys
import glob

LINKS_DIR      = "./links"
LINK_SCRAPER   = "./link_scraper.py"
MATCH_SCRAPER  = "./match_scraper.py"

WIDTH = 66

def hr(char="─"):  return char * WIDTH
def banner(text):  print(f"\n{'═'*WIDTH}\n  {text}\n{'═'*WIDTH}")
def step(text):    print(f"\n    {text}")
def ok(text):      print(f"    {text}")
def fail(text):    print(f"    {text}", file=sys.stderr)


def run(cmd: list[str]) -> int:
    print(f"\n{hr()}")
    print(f"  CMD: {' '.join(cmd)}")
    print(hr())
    return subprocess.run(cmd).returncode


def find_new_txt(before: set[str], links_dir: str) -> list[str]:
    after = set(glob.glob(os.path.join(links_dir, "*.txt")))
    return sorted(after - before)


# ── MODE 1: single URL ────────────────────────────────────────────────────────

def run_url_mode(url: str, links_dir: str) -> None:
    banner("Full Scraper  [URL mode]")
    print(f"  URL        : {url}")
    print(f"  Links dir  : {links_dir}")

    # Step 1 – link_scraper --link
    step("Running link_scraper.py --link …")
    before = set(glob.glob(os.path.join(links_dir, "*.txt")))

    rc = run([sys.executable, LINK_SCRAPER, "--link", url, "--output-dir", links_dir])
    if rc != 0:
        fail(f"link_scraper.py exited with code {rc}. Aborting.")
        sys.exit(rc)

    txt_files = find_new_txt(before, links_dir)
    if not txt_files:
        fail("link_scraper.py produced no new .txt file.")
        sys.exit(1)

    for f in txt_files:
        ok(f"Produced: {f}")

    # Step 2 – match_scraper --file
    step("Running match_scraper.py --file …")
    rc = run([sys.executable, MATCH_SCRAPER, "--file"] + txt_files)
    if rc != 0:
        fail(f"match_scraper.py exited with code {rc}.")
        sys.exit(rc)


# ── MODE 2: results .txt file ─────────────────────────────────────────────────

def run_file_mode(results_file: str, links_dir: str) -> None:
    banner("Full Scraper  [File mode]")
    print(f"  Results file : {results_file}")
    print(f"  Links dir    : {links_dir}")

    if not os.path.isfile(results_file):
        fail(f"File not found: {results_file}")
        sys.exit(1)

    # Step 1 – link_scraper --file
    step("Running link_scraper.py --file …")
    rc = run([sys.executable, LINK_SCRAPER, "--file", results_file, "--output-dir", links_dir])
    if rc != 0:
        fail(f"link_scraper.py exited with code {rc}. Aborting.")
        sys.exit(rc)

    # Verify at least one .txt was produced
    txt_files = glob.glob(os.path.join(links_dir, "*.txt"))
    if not txt_files:
        fail(f"No .txt files found in {links_dir} after link_scraper.py ran.")
        sys.exit(1)

    ok(f"Found {len(txt_files)} .txt file(s) in {links_dir}")

    # Step 2 – match_scraper --dir
    step("Running match_scraper.py --dir …")
    rc = run([sys.executable, MATCH_SCRAPER, "--dir", links_dir])
    if rc != 0:
        fail(f"match_scraper.py exited with code {rc}.")
        sys.exit(rc)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    """
    Lightweight manual parser so we can keep the positional-URL syntax from
    the original script alongside the new --file flag without argparse
    getting confused by the bare-URL positional argument.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Orchestrate link_scraper → match_scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "url",
        nargs="?",
        metavar="URL",
        help="Results-page URL to scrape (URL mode)",
    )
    group.add_argument(
        "--file", "-f",
        metavar="FILE",
        dest="from_file",
        help="Text file with one results-page URL per line (file mode)",
    )

    parser.add_argument(
        "--links-dir",
        default=LINKS_DIR,
        metavar="DIR",
        help=f"Folder used for intermediate .txt link files (default: {LINKS_DIR})",
    )

    args = parser.parse_args()

    # argparse marks the group as required=True but a bare positional with
    # nargs="?" won't trigger that — handle it explicitly.
    if not args.url and not args.from_file:
        parser.print_help()
        sys.exit(0)

    return args


def main():
    args = parse_args()

    for name, path in [("link_scraper", LINK_SCRAPER), ("match_scraper", MATCH_SCRAPER)]:
        if not os.path.isfile(path):
            fail(f"Cannot find {name} at: {path}")
            sys.exit(1)

    os.makedirs(args.links_dir, exist_ok=True)

    if args.from_file:
        run_file_mode(args.from_file, args.links_dir)
    else:
        run_url_mode(args.url, args.links_dir)

    print(f"\n{'═'*WIDTH}")
    print("  done")
    print(f"{'═'*WIDTH}\n")


if __name__ == "__main__":
    main()
