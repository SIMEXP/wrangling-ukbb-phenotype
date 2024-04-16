import pandas as pd
import numpy as np
import argparse
import util

from pathlib import Path


def oasis_npi_to_mbi(df):
    df["decreased_motivation"] = df["APA"]
    df["emotional_dysregulation"] = df["DEPD"] + df["ANX"] + df["ELAT"]
    df["impulse_dyscontrol"] = df["AGIT"] + df["IRR"] + df["MOT"]
    df["social_inappropriateness"] = df["DISN"]
    df["abnormal_perception"] = df["DEL"] + df["HALL"]
    return df


def select_columns(df):
    columns = [
        "OASISID",
        "days_to_visit",
        "decreased_motivation",
        "emotional_dysregulation",
        "impulse_dyscontrol",
        "social_inappropriateness",
        "abnormal_perception",
        "mbi_total_score",
        "mbi_status",
    ]
    df = df[columns].copy()
    return df


def oasis_merge_mbi_qc(qc_pheno_df, mbi_df):
    # Rename columns in mbi_df so they match
    mbi_df.rename(columns={"OASISID": "participant_id"}, inplace=True)
    mbi_df.rename(columns={"days_to_visit": "ses"}, inplace=True)

    # Convert ses to integer and strip the d where necessary
    mbi_df["ses_numeric"] = mbi_df["ses"].astype(int)
    qc_pheno_df["ses_numeric"] = qc_pheno_df["ses"].str.lstrip("d").astype(int)

    # Ensure ordered by session
    qc_pheno_df = qc_pheno_df.sort_values(by=["ses_numeric"])
    mbi_df = mbi_df.sort_values(by=["ses_numeric"])

    # Merge to get nearest mbi result within 6 months
    merged_df = pd.merge_asof(
        qc_pheno_df,
        mbi_df,
        by="participant_id",
        on="ses_numeric",
        direction="nearest",
        tolerance=(183),
    )

    # Handle session columns
    merged_df.drop(columns=["ses_y"], inplace=True)
    merged_df.rename(columns={"ses_x": "ses"}, inplace=True)
    merged_df.drop(columns=["ses_numeric"], inplace=True)

    return merged_df


def first_session_controls(merged_df):
    # Filter for controls
    controls_df = merged_df[merged_df["diagnosis"] == "CON"]

    # Identify the first session for each participant
    first_sessions = controls_df.groupby("participant_id")["ses"].min().reset_index()

    # Merge the first_sessions information back with the original controls_df
    # This will filter controls_df to only include rows that match the first session for each participant
    first_session_controls = pd.merge(
        controls_df, first_sessions, on=["participant_id", "ses"]
    )
    return first_session_controls


def first_session_mci_add(merged_df):
    # Filter for participants with a diagnosis of "MCI" or "ADD" and a non-empty mbi_status
    mci_add_df = merged_df[
        (merged_df["diagnosis"].isin(["MCI", "ADD"])) & merged_df["mbi_status"].notna()
    ]

    # Identify the first session for each MCI/ADD participant
    first_sessions = mci_add_df.groupby("participant_id")["ses"].min().reset_index()

    # Merge the first_sessions information back with the original mci_add_df
    # This will filter mci_add_df to only include rows that match the first session for each participant
    first_session_mci_add = pd.merge(
        mci_add_df, first_sessions, on=["participant_id", "ses"]
    )
    return first_session_mci_add


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert NPI to MBI, merge with QC and pheno data for OASIS3"
    )
    parser.add_argument("rootpath", type=Path, help="Root path")

    args = parser.parse_args()
    root_p = args.rootpath

    # Set paths
    npi_p = root_p / "data/oasis3/OASIS3_UDSb5_npiq.csv"
    qc_pheno_p = root_p / "outputs/passed_qc_master.tsv"
    output_p = root_p / "outputs/final_oasis3.tsv"

    # Load CSVs
    npi_df = pd.read_csv(npi_p)
    qc_pheno_df = pd.read_csv(qc_pheno_p, sep="\t")

    # Filter for dataset
    qc_pheno_df = qc_pheno_df[qc_pheno_df["dataset"] == "oasis3"]

    # Convert NPI to MBI and calculate total score
    mbi_df = oasis_npi_to_mbi(npi_df)
    mbi_df = util.calculate_mbi_score(mbi_df)
    mbi_df = select_columns(mbi_df)

    # Merge mbi data with qc_pheno data
    merged_df = oasis_merge_mbi_qc(qc_pheno_df, mbi_df)

    # Select scans. Find the first available session for each participant. For MCI and ADD they must have an mbi score, for controls it does not matter
    # This approach retains multiple runs from the same session
    control_df = first_session_controls(merged_df)
    mci_add_df = first_session_mci_add(merged_df)

    final_oasis = pd.concat([control_df, mci_add_df], ignore_index=True)

    # Output df
    final_oasis.to_csv(output_p, sep="\t", index=False)
