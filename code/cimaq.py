"""Load CIMA-Q data and extract demographic information.

Author: Natasha Clarke; last edit 2024-04-16

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


def merge_pheno(scan_df, diagnosis_df, socio_df, cog_df):
    df = diagnosis_df.copy()

    # Match for site
    scan_df_first = scan_df.drop_duplicates(subset="pscid", keep="first")
    df = pd.merge(
        df,
        scan_df_first[["pscid", "site_scanner"]],
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
    return df


def process_pheno(df):
    # Process the data
    df["participant_id"] = df["pscid"].astype(str)
    df["age"] = df["âge_du_participant"].astype(float)
    df["sex"] = df["sexe"].map({"femme": "female", "homme": "male"})
    df["site"] = df["site_scanner"].replace(
        {
            "Hopital Général Juif": "JGH",
        }
    )
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
    return df.copy()


def merge_qc_pheno(qc_df_filtered, pheno_df):
    # Create a numeric version of the session
    pheno_df["ses_numeric"] = pheno_df["ses"].str.replace("V", "").astype(int)
    qc_df_filtered["ses_numeric"] = (
        qc_df_filtered["ses"].str.replace("V", "").astype(int)
    )

    pheno_df["participant_id"] = pheno_df["participant_id"].astype(int)
    qc_df_filtered["participant_id"] = qc_df_filtered["participant_id"].astype(int)

    pheno_df = pheno_df.sort_values(by="ses_numeric")
    qc_df_filtered = qc_df_filtered.sort_values(by="ses_numeric")

    # Merge pheno and QC on nearest. Note that since the longest difference between scanning and pheno collection is 3 months, we don't need to set a threshold for the diagnoses
    merged_df = pd.merge_asof(
        qc_df_filtered,
        pheno_df,
        by="participant_id",  # Match participants
        on="ses_numeric",  # Find the nearest match based on session date
        direction="nearest",
    )

    # Handle site columns
    merged_df.drop(columns=["site_x"], inplace=True)
    merged_df.rename(columns={"site_y": "site"}, inplace=True)

    # Handle session columns
    merged_df.drop(columns=["ses_y"], inplace=True)
    merged_df.rename(columns={"ses_x": "ses"}, inplace=True)
    merged_df.drop(columns=["ses_numeric"], inplace=True)
    return merged_df


def merge_scanner(qc_pheno_df, scan_df):
    # Create scanner column
    scan_df["scanner"] = (
        scan_df["fabriquant"].str.replace(" ", "_")
        + "_"
        + scan_df["modele_scanner"].str.replace(" ", "_")
    ).str.lower()

    # Drop multiple entries per session for scannning data
    scan_df = scan_df.sort_values("date_du_scan").drop_duplicates(
        subset=["pscid", "no_visite"], keep="first"
    )

    qc_pheno_df["participant_id"] = qc_pheno_df["participant_id"].astype(int)
    scan_df["pscid"] = scan_df["pscid"].astype(int)

    merged_df = pd.merge(
        qc_pheno_df,
        scan_df[["pscid", "no_visite", "scanner"]],
        left_on=["participant_id", "ses"],
        right_on=["pscid", "no_visite"],
        how="left",
    )

    # Create variable of site and scanner info for ComBat
    merged_df["site_scanner"] = (
        merged_df["site"] + "_" + merged_df["scanner"]
    ).str.lower()

    return merged_df


def process_data(root_p, metadata):
    # Paths to data
    diagnosis_file_p = (
        root_p / "wrangling-phenotype/data/cimaq/22501_diagnostic_clinique.tsv"
    )
    scan_file_p = (
        root_p
        / "wrangling-phenotype/data/cimaq/dr15_20240301_sommaire_des_scans-nii.tsv"
    )  # Using the dr15 spreadsheet since there is more data available
    socio_file_p = (
        root_p
        / "wrangling-phenotype/data/cimaq/55398_informations_socio_demographiques_participant_initial.tsv"
    )
    cog_file_p = (
        root_p
        / "wrangling-phenotype/data/cimaq/84756_variables_reserve_cognitive_bartres_initial.tsv"
    )
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the CSVs
    diagnosis_df = pd.read_csv(
        diagnosis_file_p, sep="\t", parse_dates=["date_de_l_évaluation"]
    )
    scan_df = pd.read_csv(scan_file_p, sep="\t")
    socio_df = pd.read_csv(socio_file_p, sep="\t")
    cog_df = pd.read_csv(cog_file_p, sep="\t", encoding="ISO-8859-1")
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Merge different phenotypic fields
    df = merge_pheno(scan_df, diagnosis_df, socio_df, cog_df)

    # Process pheno data
    pheno_df = process_pheno(df)

    # Filter qc df for dataset
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "cimaq"].copy()

    # Merge pheno with qc
    qc_pheno_df = merge_qc_pheno(qc_df_filtered, pheno_df)

    # Merge with scan info
    qc_scan_df = merge_scanner(qc_pheno_df, scan_df)

    # Output tsv file
    qc_scan_df.to_csv(output_p / "cimaq_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "cimaq_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process CIMS-Q phenotype data, merge with QC and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to files")
    args = parser.parse_args()

    process_data(args.rootpath, metadata)
