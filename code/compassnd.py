"""Load COMPASS-ND data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-05

All input stored in `data/compassnd` folder. The content of `data` is not
included in the repository.

The data data-2024-03-05T19_55_47.299Z.csv is downloaded from https://ccna.loris.ca/. The data is not public, hence it is not included in this repository.

"""

import pandas as pd
import numpy as np
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "Identifiers",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "Initial_MRI mri_parameter_form,001_Candidate_Age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "Initial_Assessment_Screening Screening_Birth_Sex_Handedness,014_sex",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "Initial_MRI mri_parameter_form,006_site",
        "description": "Site of imaging data collection",
    },
    "diagnosis": {
        "original_field_name": "",
        "description": "Diagnosis of the participant",
        "levels": {"CON": "control", "SCHZ": "schizophrenia"},
    },
    "handedness": {
        "original_field_name": "Initial_Assessment_Screening Screening_Birth_Sex_Handedness,015_hand_preference",
        "description": "Dominant hand of the participant",
        "levels": {"right": "right", "left": "left", "ambidextrous": "ambidextrous"},
    },
}


def process_data(root_p, output_p, metadata):
    # Input file path
    data_p = root_p / "data-2024-03-05T20_52_17.741Z.csv"
    diagnosis_df = root_p / "data-2024-03-05T21_09_05.690Z.csv"

    # Load the CSV
    df = pd.read_csv(data_p)

    df.replace(".", np.nan, inplace=True)

    # Process the data
    df["participant_id"] = df["Identifiers"].astype(str)
    df["Initial_MRI mri_parameter_form,001_Candidate_Age"] = pd.to_numeric(
        df["Initial_MRI mri_parameter_form,001_Candidate_Age"], errors="coerce"
    )
    df["age"] = (
        (df["Initial_MRI mri_parameter_form,001_Candidate_Age"] / 12)
        .astype(float)
        .round(2)
    )  # Convert original age in months to years
    df["sex"] = df[
        "Initial_Assessment_Screening Screening_Birth_Sex_Handedness,014_sex"
    ].map({"female": "female", "male": "male"})
    df["site"] = df["Initial_MRI mri_parameter_form,006_site"].astype(str)
    df["site"] = df["site"].replace("nan", "")
    df["handedness"] = df[
        "Initial_Assessment_Screening Screening_Birth_Sex_Handedness,015_hand_preference"
    ].map({"right": "right", "left": "left", "ambidextrous": "ambidextrous"})
    df["education"] = df[
        "Initial_Assessment_Screening Screening_Education,010_total_years_of_schooling"
    ].astype(float)

    # Select columns
    df = df[
        ["participant_id", "age", "sex", "site", "handedness", "education"]
    ]  # Diagnosis to be added

    # Output tsv file
    df.to_csv(output_p / "compassnd_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "compassnd_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process COMPASS-ND phenotype data and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to the data files")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.rootpath, args.output, metadata)
