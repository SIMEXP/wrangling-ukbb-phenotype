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
        "original_field_name": "PTID",
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
        "original_field_name": "DX",
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
    "mmse": {
        "original_field_name": "MMSE",
        "description": "Mini Mental State Examination score",
    },
}


def process_data(root_p, output_p, metadata):
    # Paths to data
    merge_file_p = root_p / "ADNIMERGE_22Aug2023.csv"
    demo_file_p = root_p / "PTDEMOG_25Mar2024.csv"

    # Load the CSVs
    df = pd.read_csv(merge_file_p, low_memory=False, parse_dates=["EXAMDATE"])
    demo_df = pd.read_csv(demo_file_p, low_memory=False)

    # Calculate age on exam date from DOB, since only age at screening is provided in ADNIMERGE
    demo_df = demo_df.drop_duplicates(subset="PTID", keep="first")
    df = pd.merge(df, demo_df[["PTID", "PTDOB"]], on="PTID", how="left")
    df["PTDOB"] = pd.to_datetime(df["PTDOB"], format="%m/%Y")
    # Divide by np.timedelta64(1, 'Y') to convert the timedelta into years
    # Note this is an approximation, as it considers all years as 365.25 days, and 1st of the month is used for day since none provided
    df["age"] = (
        ((df["EXAMDATE"] - df["PTDOB"]) / np.timedelta64(1, "Y")).astype(float).round(1)
    )

    # Process the data
    df["diagnosis"] = df["DX"].replace({"Dementia": "ADD", "CN": "CON"})
    df["sex"] = df["PTGENDER"].map({"Female": "female", "Male": "male"})
    df["site"] = df["SITE"].astype(str)
    df["participant_id"] = df["PTID"]
    df["education"] = df["PTEDUCAT"].astype(float)
    df["ses"] = df["EXAMDATE"]
    df["mmse"] = df["MMSE"]

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
            "mmse",
        ]
    ]

    # Output tsv file
    df.to_csv(output_p / "adni_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "adni_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process ADNI phenotype data and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to the data files")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.rootpath, args.output, metadata)
