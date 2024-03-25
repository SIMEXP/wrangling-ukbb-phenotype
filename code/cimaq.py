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
        "original_field_name": "age_du_participant",
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
        "levels": {"right": "right", "left": "left", "ambidextrous": "ambidextrous"},
    },
    "education": {
        "original_field_name": "84756_nombre_annee_education",
        "description": "Years in education",
    },
    "ses": {
        "original_field_name": "no_visite",
        "description": "Session label, in this dataset it is the visit label indicating months since baseline",
    },
}


def process_data(root_p, output_p, metadata):
    # Paths to data files
    diagnosis_file_p = root_p / "22501_diagnostic_clinique.tsv"
    scan_file_p = root_p / "sommaire_des_scans.tsv"
    socio_file_p = (
        root_p / "55398_informations_socio_demographiques_participant_initial.tsv"
    )
    cog_file_p = root_p / "84756_variables_reserve_cognitive_bartres_initial.tsv"

    # Load the CSVs
    diagnosis_df = pd.read_csv(
        diagnosis_file_p, sep="\t", parse_dates=["date_de_l_évaluation"]
    )
    scan_df = pd.read_csv(scan_file_p, sep="\t")
    socio_df = pd.read_csv(socio_file_p, sep="\t")
    cog_df = pd.read_csv(cog_file_p, sep="\t", encoding="ISO-8859-1")

    # Match for site
    scan_df = scan_df.drop_duplicates(subset="pscid", keep="first")
    df = pd.merge(
        diagnosis_df,
        scan_df[["pscid", "centre"]],
        on="pscid",
        how="left",
    )

    # Match for handedness
    socio_df = socio_df.drop_duplicates(subset="PSCID", keep="first")
    df = pd.merge(
        df,
        socio_df[["PSCID", "55398_lateralite"]],
        left_on="pscid",
        right_on="PSCID",
        how="left",
    )

    # Match for education
    cog_df = cog_df.drop_duplicates(subset="PSCID", keep="first")
    df = pd.merge(
        df,
        cog_df[["PSCID", "84756_nombre_annee_education"]],
        left_on="pscid",
        right_on="PSCID",
        how="left",
    )

    # Process the data
    df["participant_id"] = df["pscid"].astype(str)
    df["age"] = df["âge_du_participant"].astype(float)
    df["sex"] = df["sexe"].map({"femme": "female", "homme": "male"})
    df["site"] = df["centre"]
    df["diagnosis"] = df["22501_diagnostic_clinique"].map(
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
        {"droitier": "right", "gaucher": "left", "ambidextre": "ambidextrous"}
    )
    df["education"] = pd.to_numeric(
        df["84756_nombre_annee_education"], errors="coerce"
    )  # This will replace the "donnée_non_disponible" entries with NaN
    df["ses"] = df["no_visite"]

    # Select columns
    df = df[
        [
            "participant_id",
            "age",
            "sex",
            "site",
            "diagnosis",
            "handedness",
            "education",
            "ses",
        ]
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
