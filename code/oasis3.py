"""Load OASIS3 data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-25

All input stored in `data/oasis3` folder. The content of `data` is not
included in the repository.

The data is downloaded from XNAT Central. The data is not public, hence it is not included in this repository. See https://www.oasis-brains.org/#data for Data Usage Agreement and how to get the data.
for access instructions.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "Subject",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "Age is included, however some fields were missing and age was rounded to the nearest year. To determine a more accurate age, we calculate it using ageAtEntry and the days from entry included in the field MR ID",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "M/F",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "None given - in the case of single site study, the site name is the dataset name",
        "description": "Site of imaging data collection",
    },
    "diagnosis": {
        "original_field_name": "Diagnoses use NACC codes, taken from the documents uds2-ivp-ded.pdf and uds3-ivp-ded.pdf. These are the PRIMARY diagnosis.",
        "description": "Diagnosis of the participant",
        "levels": {
            "ADD": "Alzheimer's disease dementia (probable)",
            "ADD(POSS)": "Alzheimer's disease dementia (possible)",
            "ALCDEM": "alcohol-related dementia",
            "ANX": "anxiety disorder",
            "COGOTH": "other cognitive/neurologic condition",
            "CON": "control",
            "CORT": "corticobasal degeneration",
            "DEMUN": "dementia of undetermined etiology",
            "DEP": "depression",
            "DLB": "dementia with lewy bodies",
            "DOWNS": "down's syndrome",
            "DYSILL": "cognitive dysfunction from medical illnesses",
            "EPIL": "epilepsy",
            "FTLD": "fronto-temporal lobe dementia",
            "HUNT": "Huntingtons disease",
            "HYCEPH": "hydrocephalus",
            "MCI": "mild cognitive impairment",
            "MEDS": "cognitive dysfunction from medications",
            "NEOP": "central nervous system neoplasm",
            "OTHPSY": "other major psychiatric illness",
            "PARK": "parkinsons disease",
            "PPA": "primary progressive aphasia",
            "PRION": "prion disease",
            "PSP": "progressive supranuclear palsy",
            "STROKE": "stroke",
            "TBI": "traumatic brain injury",
            "VASC": "vascular dementia (probable)",
            "VASC(POSS)": "vascular dementia (possible)",
            "VBI": "Vascular brain injury",
        },
    },
    "handedness": {
        "original_field_name": "HAND",
        "description": "Dominant hand of the participant",
        "levels": {"right": "right", "left": "left", "ambidextrous": "ambidextrous"},
    },
    "education": {
        "original_field_name": "EDUC",
        "description": "Years in education",
    },
    "ses": {
        "original_field_name": "contained in OASIS_session_label, e.g. in OAS30001_UDSd1_d0000, the session is d0000",
        "description": "Session label, in this dataset days from entry into study",
    },
}


def assign_diagnoses(df):
    #   Any empty diagnoses remaining do not have a clear diagnosis, e.g. no data, or "DEMENTED" with no more detail
    df["diagnosis"] = None
    #   Some rows have no primary diagnosis, so I assign these values first and if others exist they will get written over
    df.loc[(df["PROBAD"] == 1), "diagnosis"] = "ADD"
    df.loc[(df["POSSAD"] == 1), "diagnosis"] = "ADD(POSS)"
    df.loc[(df["FTD"] == 1), "diagnosis"] = "FTLD"
    df.loc[(df["PARK"] == 1), "diagnosis"] = (
        "PARK"  # Since there is no primary parkinsons column, add this last
    )

    #   Assign primary diagnoses
    df.loc[(df["anxietif"] == 1), "diagnosis"] = "ANX"
    df.loc[(df["NORMCOG"] == 1), "diagnosis"] = "CON"
    mci_columns = [
        "MCIAMEM",
        "MCIAPLUS",
        "MCIAPLAN",
        "MCIAPATT",
        "MCIAPEX",
        "MCIAPVIS",
        "MCINON1",
        "MCIN1LAN",
        "MCIN1ATT",
        "MCIN1EX",
        "MCIN1VIS",
        "MCINON2",
        "MCIN2LAN",
        "MCIN2ATT",
        "MCIN2EX",
        "MCIN2VIS",
    ]
    df.loc[df[mci_columns].sum(axis=1) > 0, "diagnosis"] = "MCI"
    df.loc[(df["PROBADIF"] == 1) | (df["alzdisif"] == 1), "diagnosis"] = "ADD"
    df.loc[(df["POSSADIF"] == 1), "diagnosis"] = "ADD(POSS)"
    df.loc[(df["DLBIF"] == 1) | (df["lbdif"] == 1), "diagnosis"] = "DLB"
    df.loc[(df["VASCIF"] == 1), "diagnosis"] = "VASC"
    df.loc[(df["VASCPSIF"] == 1), "diagnosis"] = "VASC(POSS)"
    df.loc[(df["ALCDEMIF"] == 1), "diagnosis"] = "ALCDEM"
    df.loc[(df["DEMUNIF"] == 1), "diagnosis"] = "DEMUN"
    df.loc[(df["FTDIF"] == 1) | (df["ftldnoif"] == 1), "diagnosis"] = "FTLD"
    df.loc[(df["PPAPHIF"] == 1), "diagnosis"] = "PPA"
    df.loc[(df["PSPIF"] == 1), "diagnosis"] = "PSP"
    df.loc[(df["CORTIF"] == 1), "diagnosis"] = "CORT"
    df.loc[(df["HUNTIF"] == 1), "diagnosis"] = "HUNT"
    df.loc[(df["PRIONIF"] == 1), "diagnosis"] = "PRION"
    df.loc[(df["MEDSIF"] == 1), "diagnosis"] = "MEDS"
    df.loc[(df["DYSILLIF"] == 1), "diagnosis"] = "DYSILL"
    df.loc[(df["DEPIF"] == 1), "diagnosis"] = "DEP"
    df.loc[(df["OTHPSYIF"] == 1), "diagnosis"] = "OTHPSY"
    df.loc[(df["DOWNSIF"] == 1), "diagnosis"] = "DOWNS"
    df.loc[(df["STROKIF"] == 1), "diagnosis"] = "STROKE"
    df.loc[(df["HYCEPHIF"] == 1), "diagnosis"] = "HYCEPH"
    df.loc[(df["epilepif"] == 1), "diagnosis"] = "EPIL"
    df.loc[(df["BRNINJIF"] == 1), "diagnosis"] = "TBI"
    df.loc[(df["cvdif"] == 1), "diagnosis"] = "VBI"
    df.loc[(df["NEOPIF"] == 1), "diagnosis"] = "NEOP"
    df.loc[
        (df["COGOTHIF"] == 1)
        | (df["COGOTH2F"] == 1)
        | (df["COGOTH3F"] == 1)
        | (df["othcogif"] == 1),
        "diagnosis",
    ] = "COGOTH"

    return df


def process_pheno(df):
    # Process pheno columns
    df["participant_id"] = df["OASISID"]
    df["age"] = df["age at visit"]
    df["sex"] = df["GENDER"].map({2: "female", 1: "male"})
    df["site"] = "oasis3"  # There is only one site, and no name provided
    df["handedness"] = df["HAND"].map({"R": "right", "L": "left", "B": "ambidextrous"})
    df["education"] = pd.to_numeric(df["EDUC"])
    df["ses"] = df["OASIS_session_label"].str.split("_").str[-1]

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


def merge_oasis3(qc_df_filtered, pheno_df):
    # Create a numeric version of the "days from entry" session
    pheno_df["ses_numeric"] = pheno_df["ses"].str.replace("d", "").astype(int)
    qc_df_filtered["ses_numeric"] = (
        qc_df_filtered["ses"].str.replace("d", "").astype(int)
    )

    pheno_df = pheno_df.sort_values(by="ses_numeric")
    qc_df_filtered = qc_df_filtered.sort_values(by="ses_numeric")

    # Merge pheno and QC on nearest. Rather than setting a tolerance here we calculate the difference later, since e.g. sex does not change
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

    # Calculate difference (convert to float so it handles NaNs)
    merged_df.rename(columns={"ses_numeric": "ses_scan"}, inplace=True)
    merged_df["ses_pheno"] = merged_df["ses_y"].str.replace("d", "").astype(float)
    merged_df["difference"] = (merged_df["ses_scan"] - merged_df["ses_pheno"]).abs()

    merged_df.drop(columns=["ses_y"], inplace=True)
    merged_df.rename(columns={"ses_x": "ses"}, inplace=True)
    merged_df.drop(columns=["ses_scan"], inplace=True)
    merged_df.drop(columns=["ses_pheno"], inplace=True)

    return merged_df


def apply_threshold(df):
    # For controls we allow a diagnosis within two years, for other diagnoses it must be one
    mask_con = (df["diagnosis"] == "CON") & (df["difference"] < 730.5)
    mask_other = (df["diagnosis"] != "CON") & (df["difference"] < 365.25)

    # Filter the df
    filtered_df = df[mask_con | mask_other]

    # Drop the difference column as no longer needed
    filtered_df = filtered_df.copy()
    filtered_df.drop(columns=["difference"], inplace=True)

    return filtered_df


def process_data(root_p, metadata):
    # Paths to data
    diagnosis_file_p = (
        root_p / "wrangling-phenotype/data/oasis3/OASIS3_UDSd1_diagnoses.csv"
    )
    demo_file_p = root_p / "wrangling-phenotype/data/oasis3/OASIS3_demographics.csv"
    qc_file_p = root_p / "qc_output/rest_df.tsv"
    output_p = root_p / "wrangling-phenotype/outputs"

    # Load the CSVs
    diagnosis_df = pd.read_csv(diagnosis_file_p)
    demo_df = pd.read_csv(demo_file_p)
    qc_df = pd.read_csv(qc_file_p, sep="\t", low_memory=False)

    # Merge demographics data into diagnosis data
    demo_df = demo_df.drop_duplicates(subset="OASISID", keep="first")
    df = pd.merge(diagnosis_df, demo_df, on="OASISID", how="left")

    # Assign diagnoses based on codes
    df = assign_diagnoses(df)

    # Create pheno df
    pheno_df = process_pheno(df)

    # Merge pheno with qc
    qc_df_filtered = qc_df.loc[qc_df["dataset"] == "oasis3"].copy()
    qc_pheno_df = merge_oasis3(qc_df_filtered, pheno_df)

    # Apply threshold for time between scan and phenotyping. The threshold can be changed in the function
    filtered_df = apply_threshold(qc_pheno_df)

    # Optionally, drop any scans where the subject has no diagnosis
    final_df = filtered_df.dropna(subset=["diagnosis"]).copy()

    # Output tsv file
    final_df.to_csv(output_p / "oasis3_qc_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "oasis3_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process OASIS3 phenotype data, merge with QC and output to to TSV and JSON"
    )
    parser.add_argument("rootpath", type=Path, help="Root path to files")
    args = parser.parse_args()

    process_data(args.rootpath, metadata)
