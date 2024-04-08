"""Load ADNI data and extract demographic information.

Author: Natasha Clarke; last edit 2024-04-08

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


def calculate_age(adnimerge_df, demo_df):
    # Calculate age on exam date from DOB, since only age at screening is provided in ADNIMERGE
    demo_df = demo_df.drop_duplicates(subset="PTID", keep="first")
    adnimerge_df = pd.merge(
        adnimerge_df, demo_df[["PTID", "PTDOB"]], on="PTID", how="left"
    )
    adnimerge_df["PTDOB"] = pd.to_datetime(adnimerge_df["PTDOB"], format="%m/%Y")
    # Divide by np.timedelta64(1, 'Y') to convert the timedelta into years
    # Note this is an approximation, as it considers all years as 365.25 days, and 1st of the month is used for day since none provided
    adnimerge_df["age"] = (
        ((adnimerge_df["EXAMDATE"] - adnimerge_df["PTDOB"]) / np.timedelta64(1, "Y"))
        .astype(float)
        .round(1)
    )
    return adnimerge_df


def process_pheno(adnimerge_df, adni_df):
    # In adnimerge Diagnosis == DX. These do not include screening diagnoses, so if it's missing we get the nearest one from adni_spreadsheet.csv:
    adnimerge_df.rename(
        columns={"PTID": "participant_id", "DX": "diagnosis", "EXAMDATE": "ses"},
        inplace=True,
    )
    adni_df.rename(
        columns={"Subject ID": "participant_id", "Study Date": "ses"}, inplace=True
    )

    # Select only needed columns
    adni_df = adni_df[["participant_id", "ses", "Research Group"]].copy()

    # Convert sessions to datetime
    adnimerge_df["ses"] = pd.to_datetime(adnimerge_df["ses"])
    adni_df["ses"] = pd.to_datetime(adni_df["ses"])

    # Ensure ordered by session
    adnimerge_df = adnimerge_df.sort_values(by=["ses"])
    adni_df = adni_df.sort_values(by=["ses"])

    # Merge "Research Group" from adni_spreadsheet.csv into adnimerge, within 1 year
    df = pd.merge_asof(
        adnimerge_df,
        adni_df,
        by="participant_id",
        on="ses",
        direction="nearest",
        tolerance=pd.Timedelta(days=365.25),
    )

    # Use this value if DX is missing only
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


def merge_adni(qc_df, pheno_df):
    # Filter to rows for adni
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "adni"].copy()

    # Ensure session is in datetime
    pheno_df["ses"] = pd.to_datetime(pheno_df["ses"])
    qc_df_filtered["ses"] = pd.to_datetime(qc_df_filtered["ses"])

    # Ensure sorted by session
    pheno_df = pheno_df.sort_values(by="ses")
    qc_df_filtered = qc_df_filtered.sort_values(by="ses")

    # Copy the ses column so we can use it later to calculate difference
    pheno_df["ses_pheno"] = pheno_df["ses"]

    # Merge pheno and QC on nearest. Rather than setting a tolerance here we calculate the difference later, since e.g. sex does not change
    merged_df = pd.merge_asof(
        qc_df_filtered,
        pheno_df,
        by="participant_id",  # Match participants
        on="ses",
        direction="nearest",
    )

    # Handle site columns
    merged_df.drop(columns=["site_x"], inplace=True)
    merged_df.rename(columns={"site_y": "site"}, inplace=True)

    # Calculate difference
    merged_df["difference"] = (merged_df["ses"] - merged_df["ses_pheno"]).abs()
    merged_df.drop(columns=["ses_pheno"], inplace=True)

    return merged_df


def apply_threshold(df):
    # For controls we allow a diagnosis within two years, for other diagnoses it must be one
    mask_con = (df["diagnosis"] == "CON") & (
        df["difference"] < pd.to_timedelta("365.25 days")
    )
    mask_other = (df["diagnosis"] != "CON") & (
        df["difference"] < pd.to_timedelta("730.5 days")
    )

    # Filter the df
    filtered_df = df[mask_con | mask_other]

    # Drop the difference column as no longer needed
    filtered_df = filtered_df.copy()
    filtered_df.drop(columns=["difference"], inplace=True)

    return filtered_df


def process_data(root_p, metadata):
    # Paths to data
    adnimerge_file_p = root_p / "wrangling-phenotype/data/adni/ADNIMERGE_22Aug2023.csv"
    demo_file_p = root_p / "wrangling-phenotype/data/adni/PTDEMOG_25Mar2024.csv"
    adni_file_p = root_p / "wrangling-phenotype/data/adni/adni_spreadsheet.csv"
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the CSVs
    adnimerge_df = pd.read_csv(
        adnimerge_file_p, low_memory=False, parse_dates=["EXAMDATE"]
    )
    demo_df = pd.read_csv(demo_file_p, low_memory=False)
    adni_df = pd.read_csv(adni_file_p)
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Calculate age on session date
    adnimerge_df = calculate_age(adnimerge_df, demo_df)

    # Process diagnosis data and other columns
    pheno_df = process_pheno(adnimerge_df, adni_df).copy()

    # Merge pheno with qc
    qc_pheno_df = merge_adni(qc_df, pheno_df)

    # Apply threshold for time between scan and phenotyping. The threshold can be changed in the function
    filtered_df = apply_threshold(qc_pheno_df)

    # Optionally, drop any scans where the subject has no diagnosis
    final_df = filtered_df.dropna(subset=["diagnosis"]).copy()

    # Output tsv file
    final_df.to_csv(output_p / "adni_qc_pheno.tsv", sep="\t", index=False)

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
