"""Load COMPASS-ND data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-05

All input stored in `data/compassnd` folder. The content of `data` is not
included in the repository.

The data data-2024-03-05T19_55_47.299Z.csv is downloaded from https://ccna.loris.ca/. The data is not public, hence it is not included in this repository.

"""

import pandas as pd
import numpy as np
import json
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
        "levels": ["unable to find the corresponding site names"],
    },
    "diagnosis": {
        "original_field_name": "Initial_Diagnosis_Reappraisal Reappraisal_Initial_Diagnosis_Reappraisal,008_primary_diagnosis_categories",
        "description": "Diagnosis of the participant",
        "levels": {
            "ADD": "alzheimer's disease dementia",
            "CON": "control",
            "MCI": "mild cognitive impairment",
        },
    },
    "handedness": {
        "original_field_name": "Initial_Assessment_Screening Screening_Birth_Sex_Handedness,015_hand_preference",
        "description": "Dominant hand of the participant",
        "levels": {"right": "right", "left": "left", "ambidextrous": "ambidextrous"},
    },
}


def merge_cross_sectional(qc_df_filtered, pheno_df):
    # Merge pheno information into QC, for a dataset with only one session per subject
    merged_df = pd.merge(qc_df_filtered, pheno_df, on="participant_id", how="left")

    # Handle site columns
    merged_df.drop(columns=["site_x"], inplace=True)
    merged_df.rename(columns={"site_y": "site"}, inplace=True)
    return merged_df


def process_data(metadata):
    # Set paths
    root_p = Path("/home/neuromod")
    data_p = (
        root_p / "wrangling-phenotype/data/compassnd/data-2024-03-05T20_52_17.741Z.csv"
    )
    diagnosis_p = (
        root_p / "wrangling-phenotype/data/compassnd/data-2024-03-05T21_09_05.690Z.csv"
    )
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the data
    df = pd.read_csv(data_p)
    diagnosis_df = pd.read_csv(diagnosis_p)
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    df.replace(".", np.nan, inplace=True)
    diagnosis_df.replace(".", np.nan, inplace=True)

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

    # Merge the diagnosis data
    df = df.merge(
        diagnosis_df[
            [
                "Identifiers",
                "Initial_Diagnosis_Reappraisal Reappraisal_Initial_Diagnosis_Reappraisal,008_primary_diagnosis_categories",
            ]
        ],
        left_on="Identifiers",
        right_on="Identifiers",
        how="left",
    )

    # Replace string in diagnosis col
    df["diagnosis"] = df[
        "Initial_Diagnosis_Reappraisal Reappraisal_Initial_Diagnosis_Reappraisal,008_primary_diagnosis_categories"
    ].str.replace("{@}", "_", regex=False)

    # Rename some diagnoses. TO DO: replace all so consistent across datasets. For now just doing ones I need
    df["diagnosis"] = df["diagnosis"].map(
        {
            "ad": "ADD",
            "cu": "CON",
            "mci": "MCI",
        }
    )

    # Select columns
    pheno_df = df[
        ["participant_id", "age", "sex", "site", "handedness", "education", "diagnosis"]
    ]

    # Merge pheno with QC
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "compassnd"].copy()
    qc_pheno_df = merge_cross_sectional(qc_df_filtered, pheno_df)

    # Output tsv file
    qc_pheno_df.to_csv(output_p / "compassnd_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "compassnd_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    process_data(metadata)
