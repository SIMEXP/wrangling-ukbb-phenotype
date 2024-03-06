"""Load ds000030* data and extract demographic information.

* the UCLA Consortium for Neuropsychiatric Phenomics LA5c Study

Author: Natasha Clarke; last edit 2024-02-27

All input stored in `data/ds000030` folder. The content of `data` is not
included in the repository.

The data participants.csv is downloaded from https://openneuro.org/datasets/ds000030/versions/00016.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "participant_id",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "gender",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "None given - in the case of single site study, the site name is the dataset name",
        "description": "Site of imaging data collection",
    },
    "diagnosis": {
        "original_field_name": "diagnosis",
        "description": "Diagnosis of the participant",
        "levels": {
            "CON": "control",
            "SCHZ": "schizophrenia",
            "BIPOLAR": "bipolar disorder",
            "ADHD": "Attention deficit hyperactivity disorder (ADHD)",
        },
    },
}


def process_data(root_p, output_p, metadata):
    # Path to data
    data_p = root_p / "participants.csv"

    # Load the CSV
    df = pd.read_csv(data_p)

    # Remove sub- from participant id
    df["participant_id"] = df["participant_id"].str.replace("sub-", "", regex=False)

    # Process the data
    df["age"] = df["age"].astype(float)
    df["sex"] = df["gender"].map({"F": "female", "M": "male"})
    df["site"] = "ds000030"  # There is only one site, and no name provided
    df["diagnosis"] = df["diagnosis"].map(
        {"CONTROL": "CON", "SCHZ": "SCHZ", "BIPOLAR": "BIPOLAR", "ADHD": "ADHD"}
    )

    # Select columns
    df = df[["participant_id", "age", "sex", "site", "diagnosis"]]

    # Output tsv file
    df.to_csv(output_p / "ds000030_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "ds000030_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process ds000030 phenotype data and output to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to the data files")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.rootpath, args.output, metadata)
