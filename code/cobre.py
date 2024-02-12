"""Load COBRE data and extract demographic information.

Author: Natasha Clarke; last edit 2024-02-11

All input stored in `data/cobre` folder. The content of `data` is not
included in the repository.

The data COBRE_phenotypic_data.csv is downloaded from https://fcon_1000.projects.nitrc.org/indi/retro/cobre.html. The data is not public, hence it is not included in this repository. See "NITRC Download Instructions"
for access instructions.

"""

import pandas as pd
import json
import argparse
from pathlib import Path


def process_data(csv_file_p, output_json_p):
    # Load the CSV and filter out any subjects who disenrolled
    df = pd.read_csv(csv_file_p, index_col=0)
    df = df[df["Current Age"] != "Disenrolled"]

    # Reset the index to make it a column, and rename it
    df.reset_index(inplace=True)
    df = df.rename(columns={df.columns[0]: "participant_id"})

    # Process the data
    df["participant_id"] = df["participant_id"].astype(str)
    df["age"] = df["Current Age"].astype(float)
    df["sex"] = df["Gender"].map({"Female": 0, "Male": 1})
    df["site"] = "cobre"  # There is only one site, and no name provided
    df["diagnosis"] = df["Subject Type"].map({"Control": "CON", "Patient": "SZ"})

    # Select columns
    df = df[["participant_id", "age", "sex", "site", "diagnosis"]]

    # Convert to dictionary
    data_dict = df.to_dict(orient="records")

    # Output to JSON
    with open(output_json_p / "cobre_pheno.json", "w") as f:
        json.dump(data_dict, f, indent=2)

    print(f"Data has been processed and output to {output_json_p}")


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Process COBRE CSV data and output to JSON"
    )
    parser.add_argument("datafile", type=Path, help="Path to the input CSV data file")
    parser.add_argument("output", type=Path, help="Path to the output JSON file")

    # Parse arguments
    args = parser.parse_args()

    # Call the processing function with the command-line arguments
    process_data(args.datafile, args.output)
