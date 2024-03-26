"""Load ds000030* data and extract demographic information.

* the UCLA Consortium for Neuropsychiatric Phenomics LA5c Study

Author: Natasha Clarke; last edit 2024-03-26

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


def process_pheno(df):
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
    return df


def merge_cross_sectional(qc_df_filtered, pheno_df):
    # Merge pheno information into QC, for a dataset with only one session per subject
    merged_df = pd.merge(qc_df_filtered, pheno_df, on="participant_id", how="left")

    # Handle site columns
    merged_df.drop(columns=["site_x"], inplace=True)
    merged_df.rename(columns={"site_y": "site"}, inplace=True)
    return merged_df


def process_data(root_p, metadata):
    # Paths to data
    pheno_file_p = root_p / "wrangling-phenotype/data/ds000030/participants.csv"
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the CSVs
    pheno_df = pd.read_csv(pheno_file_p)
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Process pheno df
    pheno_df = process_pheno(pheno_df)

    # Merge pheno with qc
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "ds000030"].copy()
    qc_pheno_df = merge_cross_sectional(qc_df_filtered, pheno_df)

    # Output tsv file
    qc_pheno_df.to_csv(output_p / "ds000030_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "ds000030_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process ds000030 phenotype data, merge with QC and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to files")
    args = parser.parse_args()

    process_data(args.rootpath, metadata)
