"""Load SRPBS (Open) data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-26

All input stored in `data/srpbs` folder. The content of `data` is not
included in the repository.

The data participants.tsv is downloaded from https://bicr-resource.atr.jp/srpbsopen/. The data is not public, hence it is not included in this repository. See "HOW TO DOWNLOAD THE DATA"
for access instructions.

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
        "original_field_name": "sex",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "site",
        "description": "Site of imaging data collection",
        "levels": {
            "SWA": "Showa University",
            "HUH": "Hiroshima University Hospital",
            "HRC": "Hiroshima Rehabilitation Center",
            "HKH": "Hiroshima Kajikawa Hospital",
            "COI": "Hiroshima COI",
            "KUT": "Kyoto University TimTrio",
            "KTT": "Kyoto University Trio",
            "UTO": "University of Tokyo Hospital",
            "ATT": "ATR Trio",
            "ATV": "ATR Verio",
            "CIN": "CiNet",
            "NKN": "Nishinomiya Kyouritsu Hospital",
        },
    },
    "diagnosis": {
        "original_field_name": "diag",
        "description": "Diagnosis of the participant",
        "levels": {
            "CON": "control",
            "ASD": "autism spectrum disorder",
            "MDD": "major depressive disorder",
            "OCD": "obsessive compulsive disorder",
            "SCHZ": "schizophrenia",
            "PAIN": "pain",
            "STROKE": "stroke",
            "BIPOLAR": "bipolar disorder",
            "DYSTHYMIA": "dysthmia",
            "OTHER": "other",
        },
    },
    "handedness": {
        "original_field_name": "hand",
        "description": "Dominant hand of the participant",
        "levels": {
            "right": "right",
            "left": "left",
            "ambidextrous": "ambidextrous",
        },
    },
}


def process_scanner(mri_df, df):
    # Match protocol numbers and return the scanner name
    mri_df = mri_df.transpose()

    mri_df.columns = mri_df.iloc[0]
    mri_df = mri_df[1:]

    mri_df.index = mri_df.index.astype(int)
    df["protocol"] = df["protocol"].astype(int)

    df["scanner"] = df["protocol"].map(mri_df["MRI Scanner"])
    df["scanner"] = df["scanner"].str.lower().str.replace(" ", "_")
    # Create variable of site and scanner
    df["site_scanner"] = (df["site"] + "_" + df["scanner"]).str.lower()

    return df


def process_pheno(df):
    # Remove sub- from participant id
    df["participant_id"] = df["participant_id"].str.replace("sub-", "", regex=False)

    # Process pheno columns
    df["age"] = df["age"].astype(float)
    df["sex"] = df["sex"].map({2: "female", 1: "male"})
    df["diagnosis"] = df["diag"].map(
        {
            0: "CON",
            1: "ASD",
            2: "MDD",
            3: "OCD",
            4: "SCHZ",
            5: "PAIN",
            6: "STROKE",
            7: "BIPOLAR",
            8: "DYSTHYMIA",
            99: "OTHER",
        }
    )
    df["handedness"] = df["hand"].map({1: "right", 2: "left", 0: "ambidextrous"})

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
    # Paths to data
    file_p = root_p / "wrangling-phenotype/data/srpbs/participants.tsv"
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    mri_protocol_p = root_p / "wrangling-phenotype/data/srpbs/MRI_protocols_rsMRI.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the CSV
    df = pd.read_csv(file_p, sep="\t")
    mri_df = pd.read_csv(mri_protocol_p, sep="\t", header=1)
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Get the scanner information
    df = process_scanner(mri_df, df)

    # Process pheno df
    pheno_df = process_pheno(df)

    # Merge pheno with qc
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "srpbs"].copy()
    qc_pheno_df = merge_cross_sectional(qc_df_filtered, pheno_df)

    # Output tsv file
    qc_pheno_df.to_csv(output_p / "srpbs_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "srpbs_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process SRPBS (Open) phenotype data, merge with QC and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to files")
    args = parser.parse_args()

    process_data(args.rootpath, metadata)
