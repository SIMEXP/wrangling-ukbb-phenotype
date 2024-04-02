"""Load ADNI data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-25

All input stored in `data/adni` folder. The content of `data` is not
included in the repository.

The data is downloaded from https://adni.loni.usc.edu/. The data is not public, hence it is not included in this repository. See https://adni.loni.usc.edu/data-samples/access-data/
for access instructions.

Note: adni_spreadsheet.csv was handered over to me by Desiree, who was given it by Hanad. It lists the scanning data, but I am using a spreadsheet downloaded from ADNI directly for
diagnoses.

"""

import pandas as pd
import numpy as np
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": {
            "adni_spreadsheet.csv": "Subject ID",
            "ADNIMERGE_22Aug2023.csv": "PTID",
        },
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "Calculated from PTDOB (date of birth) and EXAMDATE",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "PTGENDER",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "SITE",
        "description": "Site of imaging data collection",
        "levels": "unable to find the matching site names. Acquisition sites can be found here: https://adni.loni.usc.edu/about/centers-cores/study-sites/",
    },
    "diagnosis": {
        "original_field_name": {
            "adni_spreadsheet.csv": "Research Group",
            "ADNIMERGE_22Aug2023.csv": "DX",
        },
        "description": "Diagnosis of the participant",
        "levels": {
            "CON": "control",
            "ADD": "Alzheimer's disease dementia",
            "MCI": "mild cognitive impairment",
        },
    },
    "education": {
        "original_field_name": "PTEDUCAT",
        "description": "Years in education",
    },
    "ses": {
        "original_field_name": "EXAMDATE",
        "description": "Session label, in this dataset it is the date",
    },
}


def calculate_age(adni_df, demo_df):
    # Calculate age on exam date from DOB, since only age at screening is provided in ADNIMERGE
    demo_df = demo_df.drop_duplicates(subset="PTID", keep="first")
    adni_df = pd.merge(adni_df, demo_df[["PTID", "PTDOB"]], on="PTID", how="left")
    adni_df["PTDOB"] = pd.to_datetime(adni_df["PTDOB"], format="%m/%Y")
    # Divide by np.timedelta64(1, 'Y') to convert the timedelta into years
    # Note this is an approximation, as it considers all years as 365.25 days, and 1st of the month is used for day since none provided
    adni_df["age"] = (
        ((adni_df["EXAMDATE"] - adni_df["PTDOB"]) / np.timedelta64(1, "Y"))
        .astype(float)
        .round(1)
    )
    return adni_df


def process_pheno(df, screening_df):
    # Diagnosis == DX in ADNIMERGE. These do not include screening diagnoses, so if it's missing we get it from adni_spreadsheet.csv
    df.rename(columns={"PTID": "participant_id", "DX": "diagnosis"}, inplace=True)
    screening_df.rename(columns={"Subject ID": "participant_id"}, inplace=True)

    # Merge "Research Group" column into pheno
    df = pd.merge(
        df,
        screening_df[["participant_id", "Research Group"]],
        on="participant_id",
        how="left",
    )

    # Fill missing 'diagnosis' values with 'Research Group'
    df["diagnosis"] = df["diagnosis"].fillna(df["Research Group"])

    # Re-code diagnoses
    df["diagnosis"].replace(
        {
            "Dementia": "ADD",
            "AD": "ADD",
            "EMCI": "MCI",
            "LMCI": "MCI",
            "CN": "CON",
            "SMC": "CON",
        },
        inplace=True,
    )  # SMC (subjective impairment) was only used at screening and all went on to be classed as controls

    df["sex"] = df["PTGENDER"].map({"Female": "female", "Male": "male"})
    df["site"] = df["SITE"].astype(str)
    df["education"] = df["PTEDUCAT"].astype(float)
    df["ses"] = df["EXAMDATE"]

    df["participant_id"] = df["participant_id"].str.replace(
        "_", "", regex=False
    )  # So it matches the id in MRI file names

    # Select columns
    df = df[
        [
            "participant_id",
            "age",
            "sex",
            "site",
            "diagnosis",
            "education",
            "ses",
        ]
    ]
    return df


def merge_adni(qc_df_filtered, pheno_df):
    pheno_df["ses"] = pd.to_datetime(pheno_df["ses"])
    qc_df_filtered["ses"] = pd.to_datetime(qc_df_filtered["ses"])

    pheno_df = pheno_df.sort_values(by="ses")
    qc_df_filtered = qc_df_filtered.sort_values(by="ses")

    merged_df = pd.merge_asof(
        qc_df_filtered,
        pheno_df,
        by="participant_id",  # Match participants
        on="ses",  # Find the nearest match based on session date
        direction="nearest",
    )  # tolerance=pd.Timedelta(days=365),

    # Handle site columns
    merged_df.drop(columns=["site_x"], inplace=True)
    merged_df.rename(columns={"site_y": "site"}, inplace=True)

    return merged_df


def process_data(root_p, metadata):
    # Paths to data
    adni_file_p = root_p / "wrangling-phenotype/data/adni/ADNIMERGE_22Aug2023.csv"
    demo_file_p = root_p / "wrangling-phenotype/data/adni/PTDEMOG_25Mar2024.csv"
    screening_file_p = root_p / "wrangling-phenotype/data/adni/adni_spreadsheet.csv"
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the CSVs
    adni_df = pd.read_csv(adni_file_p, low_memory=False, parse_dates=["EXAMDATE"])
    demo_df = pd.read_csv(demo_file_p, low_memory=False)
    screening_df = pd.read_csv(screening_file_p)
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Calculate age on session date
    adni_df = calculate_age(adni_df, demo_df)

    # Process diagnosis data and other columns
    pheno_df = process_pheno(adni_df, screening_df).copy()

    # Merge pheno with qc
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "adni"].copy()
    qc_pheno_df = merge_adni(qc_df_filtered, pheno_df)

    # Output tsv file
    qc_pheno_df.to_csv(output_p / "adni_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "adni_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process ADNI phenotype data, merge with QC and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to files")
    args = parser.parse_args()

    process_data(args.rootpath, metadata)
