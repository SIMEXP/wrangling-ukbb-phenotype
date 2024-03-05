"""Load CIMA-Q data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-05

All input stored in `data/cimaq` folder. The content of `data` is not
included in the repository.

The data participants.tsv is downloaded from http://loris.cima-q.ca/. The data is not public, hence it is not included in this repository. See "HOW TO DOWNLOAD THE DATA"
for access instructions.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "pscid",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "sexe",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "centre",
        "description": "Site of imaging data collection",
        "levels": {
            "CHUS": "Centre hospitalier universitaire de Sherbrooke",
            "CINQ": "Consortium d'Imagerie en Neurosciences et Sante Mentale de Quebec",
            "IUGM": "Institut universitaire de geriatrie de Montreal",
            "JGH": "Jewish General Hospital",
        },
    },
    "diagnosis": {
        "original_field_name": "22501_diagnostic_clinique",
        "description": "Diagnosis of the participant",
        "levels": {
            "ADD(M)": "alzheimer's disease dementia (mild)",
            "CON": "control",
            "EMCI": "early mild cognitive impairment",
            "LMCI": "late mild cognitive impairment",
            "OTHER": "other",
            "SCD": "subjective cognitive decline",
        },
    },
    "handedness": {
        "original_field_name": "55398_lateralite",
        "description": "Dominant hand of the participant",
        "levels": {"right": "right", "left": "left", "both": "both"},
    },
    "education": {
        "original_field_name": "84756_nombre_annee_education",
        "description": "Years in education",
    },
}


def find_closest_diagnosis(scan_row, diagnosis_df):
    pscid = scan_row["pscid"]
    scan_date = scan_row["date"]

    # Filter to evaluations for the same participant and where the evaluation date is not NULL
    participant_df = diagnosis_df[
        (diagnosis_df["pscid"] == pscid) & diagnosis_df["date_de_l_évaluation"].notna()
    ].copy()

    # Find the closest date
    if not participant_df.empty:
        # Compute the absolute difference in days between the scan date and diagnosis evaluation dates
        participant_df["date_diff"] = (
            participant_df["date_de_l_évaluation"].sub(scan_date).dt.days.abs()
        )

        # Find the diagnosis with the smallest date difference
        closest_date = participant_df.loc[participant_df["date_diff"].idxmin()]

        # Return the diagnosis, date and numebr of days. Also return the sex info
        return (
            closest_date["22501_diagnostic_clinique"],
            closest_date["date_de_l_évaluation"],
            closest_date["date_diff"],
            closest_date["sexe"],
        )
    else:
        # Return None for each expected value to maintain consistency
        return None, None, None


def process_data(root_p, output_p, metadata):
    # Paths to different data files
    scan_file_p = root_p / "sommaire_des_scans.tsv"
    diagnosis_file_p = root_p / "22501_diagnostic_clinique.tsv"
    socio_file_p = (
        root_p / "55398_informations_socio_demographiques_participant_initial.tsv"
    )
    edu_file_p = root_p / "84756_variables_reserve_cognitive_bartres_initial.tsv"

    # Load the CSVs
    df = pd.read_csv(scan_file_p, sep="\t", parse_dates=["date"])
    diagnosis_df = pd.read_csv(
        diagnosis_file_p, sep="\t", parse_dates=["date_de_l_évaluation"]
    )
    socio_df = pd.read_csv(socio_file_p, sep="\t")
    edu_df = pd.read_csv(edu_file_p, sep="\t", encoding="ISO-8859-1")

    # Select only resting state data
    df = df[df["nii_protocole"] == "task-rest"]  # Run again for task-memory

    # Apply function to match diagnoses according to closest scan date, and split the results into new columns
    result = df.apply(
        lambda row: pd.Series(find_closest_diagnosis(row, diagnosis_df)), axis=1
    )
    df[["diagnosis", "matched_evaluation_date", "date_diff", "sex"]] = (
        result  # Returning these for now because at a later date we may want to drop e.g participants with diagnoses outside a certain window
    )

    # Match with df for handedness
    df = pd.merge(
        df,
        socio_df[["PSCID", "55398_lateralite"]],
        left_on="pscid",
        right_on="PSCID",
        how="left",
    )

    # Match with df for education
    df = pd.merge(
        df,
        edu_df[["PSCID", "84756_nombre_annee_education"]],
        left_on="pscid",
        right_on="PSCID",
        how="left",
    )

    # Process the data
    df["participant_id"] = df["pscid"].astype(str)
    df["age"] = df["age"].astype(float)
    df["sex"] = df["sex"].map({"femme": "female", "homme": "male"})
    df["site"] = df["centre"]
    df["diagnosis"] = df["diagnosis"].map(
        {
            "démence_de_type_alzheimer-légère": "ADD(M)",
            "cognitivement_sain_(cs)": "CON",
            "trouble_cognitif_léger_précoce": "EMCI",
            "trouble_cognitif_léger_tardif": "LMCI",
            "autre": "OTHER",
            "troubles_subjectifs_de_cognition": "SCD",
        }
    )
    df["handedness"] = df["55398_lateralite"].map(
        {"droitier": "right", "gaucher": "left", "ambidextre": "both"}
    )
    df["education"] = pd.to_numeric(
        df["84756_nombre_annee_education"], errors="coerce"
    )  # This will replace the "donnée_non_disponible" entries with NaN

    # Select columns
    df = df[
        ["participant_id", "age", "sex", "site", "diagnosis", "handedness", "education"]
    ]

    # Output tsv file
    df.to_csv(output_p / "cimaq_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "cimaq_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process CIMA-Q phenotype data and output to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to the data files")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.rootpath, args.output, metadata)
