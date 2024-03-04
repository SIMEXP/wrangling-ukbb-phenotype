"""Load COBRE data and extract demographic information.

Author: Natasha Clarke; last edit 2024-02-27

All input stored in `data/cobre` folder. The content of `data` is not
included in the repository.

The data COBRE_phenotypic_data.csv is downloaded from https://fcon_1000.projects.nitrc.org/indi/retro/cobre.html. The data is not public, hence it is not included in this repository. See "NITRC Download Instructions"
for access instructions.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "Column header is empty in original data",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "Current Age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "Gender",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "None given - in the case of single site study, the site name is the dataset name",
        "description": "Site of imaging data collection",
    },
    "diagnosis": {
        "original_field_name": "Subject Type",
        "description": "Diagnosis of the participant",
        "levels": {"CON": "control", "SCHZ": "schizophrenia"},
    },
    "handedness": {
        "original_field_name": "Handedness",
        "description": "Dominant hand of the participant",
        "levels": {"right": "right", "left": "left", "both": "both"},
    },
}


def process_data(csv_file_p, output_p, metadata):
    # Load the CSV and rename first column
    df = pd.read_csv(csv_file_p, dtype=str)
    df.rename(columns={df.columns[0]: "participant_id"}, inplace=True)

    # Filter out any subjects who disenrolled
    df = df[df["Current Age"] != "Disenrolled"]

    # Process the data
    df["participant_id"] = df["participant_id"].astype(str)
    df["age"] = df["Current Age"].astype(float)
    df["sex"] = df["Gender"].map({"Female": "female", "Male": "male"})
    df["site"] = "cobre"  # There is only one site, and no name provided
    df["diagnosis"] = df["Subject Type"].map({"Control": "CON", "Patient": "SCHZ"})
    df["handedness"] = df["Handedness"].map(
        {"Right": "right", "Left": "left", "Both": "both"}
    )

    # Select columns
    df = df[["participant_id", "age", "sex", "site", "diagnosis", "handedness"]]

    # Output tsv file
    df.to_csv(output_p / "cobre_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "cobre_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process COBRE phenotype data and output to to TSV and JSON"
    )
    parser.add_argument("datafile", type=Path, help="Path to the input CSV data file")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.datafile, args.output, metadata)
