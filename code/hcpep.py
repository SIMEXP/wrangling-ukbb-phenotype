"""Load HCP-EP data and extract demographic information.

Author: Natasha Clarke; last edit 2024-02-14

All input stored in `data/hcpep` folder. The content of `data` is not
included in the repository.

The data ndar_subject01.txt is downloaded from https://nda.nih.gov/. The data is not public, hence it is not included in this repository.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "src_subject_id",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "interview_age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "sex",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "site",
        "description": "Site of imaging data collection",
        "levels": {
            "IU": "Indiana University",
            "BWH": "Brigham and Women's Hospital",
            "MGH": "Massachusetts General Hospital",
            "MLH": "McLean Hospital",
        },
    },
    "diagnosis": {
        "original_field_name": "phenotype",
        "description": "Diagnosis of the participant",
        "levels": {"CON": "control", "PSYC": "psychosis"},
    },
}


def process_data(root_p, output_p, metadata):
    # Path to data
    data_p = root_p / "ndar_subject01.txt"

    # Load the data
    df = pd.read_csv(data_p, delimiter="\t", header=[0, 1])
    df.columns = df.columns.droplevel(1)  # Drop the second header

    # Process the data
    df["participant_id"] = df["src_subject_id"].astype(str)
    df["age"] = (
        (df["interview_age"] / 12).astype(float).round(2)
    )  # Convert original age in months to years
    df["sex"] = df["sex"].map({"F": "female", "M": "male"})
    df["site"] = df["site"].map(
        {
            "Indiana University": "IU",
            "Brigham and Women's Hospital": "BWH",
            "Massachusetts General Hospital": "MGH",
            "McLean Hospital": "MLH",
        }
    )
    df["diagnosis"] = df["phenotype"].map({"Control": "CON", "Patient": "PSYC"})

    # Select columns
    df = df[["participant_id", "age", "sex", "site", "diagnosis"]]

    # Output tsv file
    df.to_csv(output_p / "hcpep_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "hcpep_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process HCP-EP phenotype data and output to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to the data files")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.rootpath, args.output, metadata)
