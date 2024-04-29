import pandas as pd
import numpy as np
import argparse
import util

from pathlib import Path


def process_adni_mbi(qc_pheno_df):
    # Set path and load data
    npi_p = root_p / "data/adni/NPI_22Aug2023.csv"
    npi_df = pd.read_csv(npi_p)

    # Filter for dataset
    qc_pheno_df = qc_pheno_df[qc_pheno_df["dataset"] == "adni"]

    # Convert NPI to MBI and calculate total score
    mbi_df = util.adni_npi_to_mbi(npi_df)
    mbi_df = util.calculate_mbi_score(mbi_df)
    mbi_df = util.adni_select_columns(mbi_df)

    # Merge mbi data with qc_pheno data
    merged_df = util.adni_merge_mbi_qc(qc_pheno_df, mbi_df)

    # Select scans. For controls, we take the first available scan. For MCI and ADD, take the first with an MBI score
    final_adni = (
        merged_df.groupby(["participant_id"], as_index=False)
        .apply(util.select_row)
        .reset_index(drop=True)
    )

    return final_adni


def process_cimaq_mbi(qc_pheno_df):
    # Set path and load data
    npi_p = root_p / "data/cimaq/22901_inventaire_neuropsychiatrique_q.tsv"
    npi_df = pd.read_csv(npi_p, sep="\t")

    # Filter for dataset
    qc_pheno_df = qc_pheno_df[qc_pheno_df["dataset"] == "cimaq"]

    # Convert NPI to MBI and calculate total score
    npi_df = util.cimaq_map_values(npi_df)
    mbi_df = util.cimaq_npi_to_mbi(npi_df)
    mbi_df = util.calculate_mbi_score(mbi_df)
    mbi_df = util.cimaq_select_columns(mbi_df)

    # Merge mbi data with qc_pheno data
    merged_df = util.cimaq_merge_mbi_qc(qc_pheno_df, mbi_df)

    # Select scans. For controls, we take the first available scan. For MCI and ADD, take the first with an MBI score
    final_cimaq = (
        merged_df.groupby(["participant_id"], as_index=False)
        .apply(util.select_row)
        .reset_index(drop=True)
    )

    return final_cimaq


def process_oasis3_mbi(qc_pheno_df):
    # Set path and load data
    npi_p = root_p / "data/oasis3/OASIS3_UDSb5_npiq.csv"
    npi_df = pd.read_csv(npi_p)

    # Filter for dataset
    qc_pheno_df = qc_pheno_df[qc_pheno_df["dataset"] == "oasis3"]

    # Convert NPI to MBI and calculate total score
    mbi_df = util.oasis3_npi_to_mbi(npi_df)
    mbi_df = util.calculate_mbi_score(mbi_df)
    mbi_df = util.oasis3_select_columns(mbi_df)

    # Merge mbi data with qc_pheno data
    merged_df = util.oasis3_merge_mbi_qc(qc_pheno_df, mbi_df)

    # Select scans. Find the first available session for each participant. For MCI and ADD they must have an mbi score, for controls it does not matter
    # This approach retains multiple runs from the same session
    control_df = util.first_session_controls(merged_df)
    mci_add_df = util.first_session_mci_add(merged_df)

    final_oasis3 = pd.concat([control_df, mci_add_df], ignore_index=True)

    return final_oasis3


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert NPI to MBI, merge with QC and pheno data for ADNI, CIMA-Q and OASIS3"
    )
    parser.add_argument("rootpath", type=Path, help="Root path")

    args = parser.parse_args()
    root_p = args.rootpath

    # Load passed_qc_master once
    qc_pheno_p = root_p / "outputs/passed_qc_master.tsv"
    qc_pheno_df = pd.read_csv(qc_pheno_p, sep="\t", low_memory=False)

    # Process mbi scores for specific datasets
    final_adni = process_adni_mbi(qc_pheno_df)
    final_cimaq = process_cimaq_mbi(qc_pheno_df)
    final_oasis3 = process_oasis3_mbi(qc_pheno_df)

    # Remove existing data for specific datasets from qc_pheno_df
    remaining_qc_pheno_df = qc_pheno_df[
        ~qc_pheno_df["dataset"].isin(["adni", "cimaq", "oasis3"])
    ]

    # Concatenate the remaining with the new processed mbi data
    updated_qc_pheno_df = pd.concat(
        [remaining_qc_pheno_df, final_adni, final_cimaq, final_oasis3],
        ignore_index=True,
    )

    # Output df
    output_p = root_p / "outputs/final_master_pheno.tsv"
    updated_qc_pheno_df.to_csv(output_p, sep="\t", index=False)
