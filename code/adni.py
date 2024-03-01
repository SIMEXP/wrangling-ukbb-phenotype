"""Load ADNI data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-01

All input stored in `data/adni` folder. The content of `data` is not
included in the repository.

The data is downloaded from https://adni.loni.usc.edu/. The data is not public, hence it is not included in this repository. See https://adni.loni.usc.edu/data-samples/access-data/
for access instructions.

Note: adni_spreadsheet.csv was handered over to me by Desiree, who was given it by Hanad. It lists the scanning data, but I am using a spreadsheet downloaded from ADNI directly for
diagnoses.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "Subject ID in adni_spreadsheet.csv (PTID in ADNIMERGE_22Aug2023.csv)",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "Age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "Sex",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "Site is provided in the subject ID, e.g. for 011_S_0002 the site is 11",
        "description": "Site of imaging data collection",
        "levels": "unable to find the matching site names. Acquisition sites can be found here: https://adni.loni.usc.edu/about/centers-cores/study-sites/",
    },
    "diagnosis": {
        "original_field_name": "DX in ADNIMERGE_22Aug2023.csv, or Research Group in adni_spreadsheet.csv",
        "description": "Diagnosis of the participant",
        "levels": {
            "CON": "control",
            "ADD": "Alzheimer's disease dementia",
            "MCI": "mild cognitive impairment",
            "Patient": "unknown",
        },
    },
}


def find_closest_date(participant_df, scan_date):
    # Compute the absolute difference in days between the scan date and diagnosis dates
    participant_df["date_diff"] = (
        participant_df["EXAMDATE"].sub(scan_date).dt.days.abs()
    )

    # Sort by date difference
    participant_df.sort_values(by="date_diff", inplace=True)

    # Filter for rows where DX is not blank, maintaining the order
    valid_diagnosis_df = participant_df[
        participant_df["DX"].notna() & (participant_df["DX"] != "")
    ]

    # Find the closest date with a valid diagnosis, if available
    if not valid_diagnosis_df.empty:
        closest_valid_diagnosis = valid_diagnosis_df.iloc[
            0
        ]  # First row is the closest with a valid DX
        return (
            closest_valid_diagnosis["DX"],
            closest_valid_diagnosis["EXAMDATE"],
            closest_valid_diagnosis["date_diff"],
        )
    else:
        # Return None if no valid diagnosis is found
        return None, None, None


def find_closest_diagnosis(scan_row, diagnosis_df):
    subject_id = scan_row["Subject ID"]
    scan_date = scan_row["Study Date"]

    # Filter to evaluations for the same participant where the evaluation date is not NULL
    participant_df = diagnosis_df[
        (diagnosis_df["PTID"] == subject_id) & diagnosis_df["EXAMDATE"].notna()
    ].copy()

    if not participant_df.empty:
        return find_closest_date(participant_df, scan_date)
    else:
        # Return None if no matching participant entries were found
        return None, None, None


def process_diagnosis_data(scan_df, diagnosis_df):
    # Apply function to match diagnoses according to closest scan date, and split the results into new columns
    result = scan_df.apply(
        lambda row: pd.Series(find_closest_diagnosis(row, diagnosis_df)), axis=1
    )
    scan_df[["diagnosis", "matched_evaluation_date", "date_diff"]] = (
        result  # Returning these for now because at a later date we may want to drop e.g participants with diagnoses outside a certain window
    )

    # For those who only have screening visit, copy that diagnosis to column
    scan_df.loc[scan_df["matched_evaluation_date"].isna(), "diagnosis"] = scan_df[
        "Research Group"
    ]

    # Map values
    scan_df["diagnosis"] = scan_df["diagnosis"].replace(
        {"CN": "CON", "EMCI": "MCI", "LMCI": "MCI", "Dementia": "ADD"}
    )

    return scan_df


def process_data(scan_file_p, diagnosis_file_p, output_p, metadata):
    # Load the CSVs
    scan_df = pd.read_csv(scan_file_p, index_col=0, parse_dates=["Study Date"])
    diagnosis_df = pd.read_csv(
        diagnosis_file_p, low_memory=False, parse_dates=["EXAMDATE"]
    )

    # Match diagnoses
    df = process_diagnosis_data(scan_df, diagnosis_df)

    # Process the data
    df["age"] = df["Age"].astype(float)
    df["sex"] = df["Sex"].map({"F": "female", "M": "male"})
    df["site"] = (
        df["Subject ID"].str.split("_").str[0].str.lstrip("0")
    )  # Extract site variable from ID and remove leading zeros
    df["participant_id"] = df["Subject ID"].str.replace("_", "", regex=False)

    # Select columns
    df = df[["participant_id", "age", "sex", "site", "diagnosis"]]

    # Sort df
    df = df.sort_values(by=["participant_id", "age"])

    # Output tsv file
    df.to_csv(output_p / "adni_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "adni_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process ADNI phenotype data and output to to TSV and JSON"
    )
    parser.add_argument("scanfile", type=Path, help="Path to adni_spreadsheet.csv")
    parser.add_argument("diagfile", type=Path, help="Path to ADNIMERGE_22Aug2023.csv")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.scanfile, args.diagfile, args.output, metadata)
