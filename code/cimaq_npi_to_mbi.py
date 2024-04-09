import pandas as pd
import numpy as np
import argparse
import util
from pathlib import Path


def map_values(df):
    # Drop rows with some data unavailable
    df = df[df["22901_score"] != "donnée_non_disponible"].copy()

    # Map scores to numerical values
    mapping = {"0_non": 0, "1_oui_léger": 1, "2_oui_modéré": 2, "3_oui_sévère": 3}

    columns_to_map = [
        "22901_apathie",
        "22901_depression_dysphorie",
        "22901_anxiete",
        "22901_euphorie",
        "22901_agitation_aggressivite",
        "22901_irritabilite",
        "22901_comp_moteur_aberrant",
        "22901_impulsivite",
        "22901_idees_delirantes",
        "22901_hallucinations",
    ]

    for column in columns_to_map:
        df[column] = df[column].map(mapping)
    return df


def cimaq_npi_to_mbi(df):
    df["decreased_motivation"] = df["22901_apathie"]
    df["emotional_dysregulation"] = (
        df["22901_depression_dysphorie"] + df["22901_anxiete"] + df["22901_euphorie"]
    )
    df["impulse_dyscontrol"] = (
        df["22901_agitation_aggressivite"]
        + df["22901_irritabilite"]
        + df["22901_comp_moteur_aberrant"]
    )
    df["social_inappropriateness"] = df["22901_impulsivite"]
    df["abnormal_perception"] = (
        df["22901_idees_delirantes"] + df["22901_hallucinations"]
    )
    return df


def select_columns(df):
    columns = [
        "pscid",
        "no_visite",
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


def cimaq_merge_mbi_qc(qc_pheno_df, mbi_df):
    # Rename columns in mbi_df so they match
    mbi_df.rename(columns={"pscid": "participant_id"}, inplace=True)
    mbi_df.rename(columns={"no_visite": "ses"}, inplace=True)

    # Format id
    mbi_df["participant_id"] = mbi_df["participant_id"].astype(int)
    qc_pheno_df["participant_id"] = qc_pheno_df["participant_id"].astype(int)

    # Strip the 'V' from ses and convert to integer
    mbi_df["ses_numeric"] = mbi_df["ses"].str.lstrip("V").astype(int)
    qc_pheno_df["ses_numeric"] = qc_pheno_df["ses"].str.lstrip("V").astype(int)

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
        tolerance=(6),
    )

    # Handle session columns
    merged_df.drop(columns=["ses_y"], inplace=True)
    merged_df.rename(columns={"ses_x": "ses"}, inplace=True)
    merged_df.drop(columns=["ses_numeric"], inplace=True)

    return merged_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert NPI to MBI, merge with QC and pheno data for OASIS3"
    )
    parser.add_argument("rootpath", type=Path, help="Root path")

    args = parser.parse_args()
    root_p = args.rootpath

    # Set paths
    npi_p = root_p / "data/cimaq/22901_inventaire_neuropsychiatrique_q.tsv"
    qc_pheno_p = root_p / "outputs/cimaq_qc_pheno.tsv"
    output_p = root_p / "outputs/final_cimaq.tsv"

    # Load CSVs
    npi_df = pd.read_csv(npi_p, sep="\t")
    qc_pheno_df = pd.read_csv(qc_pheno_p, sep="\t")

    # Convert NPI to MBI and calculate total score
    npi_df = map_values(npi_df)
    mbi_df = cimaq_npi_to_mbi(npi_df)
    mbi_df = util.calculate_mbi_score(mbi_df)
    mbi_df = select_columns(mbi_df)

    merged_df = cimaq_merge_mbi_qc(qc_pheno_df, mbi_df)

    # Select scans. For controls, we take the first available scan. For MCI and ADD, take the first with an MBI score
    final_cimaq = (
        merged_df.groupby(["participant_id"], as_index=False)
        .apply(util.select_row)
        .reset_index(drop=True)
    )

    # Output df
    final_cimaq.to_csv(output_p, sep="\t", index=False)
