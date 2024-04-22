"""Load COBRE data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-26

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
        "levels": {"right": "right", "left": "left", "ambidextrous": "ambidextrous"},
    },
}


def process_pheno(df):
    # Add name for id column
    df.rename(columns={df.columns[0]: "participant_id"}, inplace=True)

    # Filter out any subjects who disenrolled (there is no pheno data for them)
    df = df[df["Current Age"] != "Disenrolled"].copy()

    # Process the data
    df["participant_id"] = df["participant_id"].astype(str)
    df["age"] = df["Current Age"].astype(float)
    df["sex"] = df["Gender"].map({"Female": "female", "Male": "male"})
    df["site"] = "cobre"  # There is only one site, and no name provided
    df["scanner"] = "siemens_triotim"  # Given in COBRE_parameters_mprage.csv
    df["site_scanner"] = df["site"] + "_" + df["scanner"]
    df["diagnosis"] = df["Subject Type"].map({"Control": "CON", "Patient": "SCHZ"})
    df["handedness"] = df["Handedness"].map(
        {"Right": "right", "Left": "left", "Both": "ambidextrous"}
    )

    # Select columns
    df = df[
        [
            "participant_id",
            "age",
            "sex",
            "site",
            "diagnosis",
            "handedness",
            "scanner",
            "site_scanner",
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
    # Path to data
    file_p = root_p / "wrangling-phenotype/data/cobre/COBRE_phenotypic_data.csv"
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the CSVs
    df = pd.read_csv(file_p, dtype=str)
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Process pheno df
    pheno_df = process_pheno(df)

    # Merge pheno with qc
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "cobre"].copy()
    qc_pheno_df = merge_cross_sectional(qc_df_filtered, pheno_df)

    # Output tsv file
    qc_pheno_df.to_csv(output_p / "cobre_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "cobre_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process COBRE phenotype data, merge with QC and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to files")
    args = parser.parse_args()

    process_data(args.rootpath, metadata)
