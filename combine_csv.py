import os
import pandas as pd
import argparse
import re

def extract_season(filename):
    match = re.search(r"\d{4}(?:-\d{4})?", filename)
    return match.group(0) if match else "unknown"

def combine_csv(folder_path, output_file, add_season=False):
    dataframes = []

    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            file_path = os.path.join(folder_path, file)
            print(f"Reading: {file_path}")

            df = pd.read_csv(file_path)

            # Only add season if flag is enabled
            if add_season:
                season = extract_season(file)
                df.insert(2, "season", season)

            dataframes.append(df)

    if not dataframes:
        print("No CSV files found.")
        return

    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.to_csv(output_file, index=False)

    print(f"\nAll CSV files combined into: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine CSV files in a folder")

    parser.add_argument(
        "--folder",
        required=True,
        help="Path to folder containing CSV files"
    )

    parser.add_argument(
        "--output",
        default="combined.csv",
        help="Output CSV file name (default: combined.csv)"
    )

    
    parser.add_argument(
        "--season",
        action="store_true",
        help="Add season column extracted from filename"
    )

    args = parser.parse_args()

    combine_csv(args.folder, args.output, args.season)