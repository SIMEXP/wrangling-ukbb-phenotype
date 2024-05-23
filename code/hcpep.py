"""Load HCP-EP data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-26

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


def process_pheno(df):
    df.columns = df.columns.droplevel(1)  # Drop the second header
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
    df["scanner"] = (
        "siemens_magnetom_prisma"  # Specified in HCP-EP_Release_1.1_Manual.pdf (+ correspondence with study team. articipants were scanned at one of two sites, not necessarily their site, but this data is not released so I think this is the best we can do)
    )
    df["diagnosis"] = df["phenotype"].map({"Control": "CON", "Patient": "PSYC"})

    # Select columns
    df = df[
        [
            "participant_id",
            "age",
            "sex",
            "site",
            "diagnosis",
            "scanner",
        ]
    ]
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
    file_p = root_p / "wrangling-phenotype/data/hcpep/ndar_subject01.txt"
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the data
    df = pd.read_csv(file_p, delimiter="\t", header=[0, 1])
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Process pheno df
    pheno_df = process_pheno(df)

    # Merge pheno with qc
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "hcpep"].copy()
    qc_pheno_df = merge_cross_sectional(qc_df_filtered, pheno_df)

    # Output tsv file
    qc_pheno_df.to_csv(output_p / "hcpep_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "hcpep_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process HCP-EP phenotype data, merge with QC and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to files")
    args = parser.parse_args()

    process_data(args.rootpath, metadata)
