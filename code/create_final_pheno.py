import pandas as pd
import numpy as np
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

    # Merge mbi data with qc_pheno data. Note that for controls with an mbi not in window, they are left blank which is fine
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


def process_compassnd_mbi(qc_pheno_df):
    # Set path and load data
    npi_p = root_p / "data/compassnd/data-2024-07-10T22_03_30.029Z.csv"
    npi_df = pd.read_csv(npi_p)

    # Filter for dataset
    qc_pheno_df = qc_pheno_df[qc_pheno_df["dataset"] == "compassnd"]

    mbi_df = util.compassnd_npi_to_mbi(npi_df)
    mbi_df = util.calculate_mbi_score(mbi_df)
    mbi_df = util.compassnd_select_columns(mbi_df)

    # Convert age in months to years
    mbi_df["age"] = (
        (
            mbi_df[
                "Clinical_Assessment PI_Neuropsychiatric_Inventory_Questionnaire,001_Candidate_Age"
            ]
            / 12
        )
        .astype(float)
        .round(2)
    )

    mbi_df.drop(
        columns=[
            "Clinical_Assessment PI_Neuropsychiatric_Inventory_Questionnaire,001_Candidate_Age"
        ],
        inplace=True,
    )

    # Merge mbi data with qc_pheno data
    merged_df = util.compassnd_merge_mbi_qc(qc_pheno_df, mbi_df)

    # Select scans. Find the first available session for each participant. For MCI and ADD they must have an mbi score, for controls it does not matter
    # This approach retains multiple runs from the same session
    control_df = util.first_session_controls(merged_df)
    mci_add_df = util.first_session_mci_add(merged_df)

    final_compassnd = pd.concat([control_df, mci_add_df], ignore_index=True)

    return final_compassnd


def assign_mbi_group(row):
    if row["diagnosis"] in ["ADD"]:
        if row["mbi_status"] == 1:
            return "ADD+"
        elif row["mbi_status"] == 0:
            return "ADD-"
    elif row["diagnosis"] in ["MCI"]:
        if row["mbi_status"] == 1:
            return "MCI+"
        elif row["mbi_status"] == 0:
            return "MCI-"
    elif row["diagnosis"] == "CON":
        return "CON-ADD"


def assign_sz_group(row):
    if row["diagnosis"] in ["SCHZ"]:
        return "SCHZ"
    elif row["diagnosis"] == "CON":
        return "CON-SCHZ"


def create_final_ad_df(qc_pheno_df):
    # Process mbi scores for specific datasets
    final_adni = process_adni_mbi(qc_pheno_df)
    final_cimaq = process_cimaq_mbi(qc_pheno_df)
    final_oasis3 = process_oasis3_mbi(qc_pheno_df)
    final_compassnd = process_compassnd_mbi(qc_pheno_df)

    # Concatenate these Alzheimer datasets
    ad_datasets_df = pd.concat(
        [final_adni, final_cimaq, final_oasis3, final_compassnd],
        ignore_index=True,
    )

    # Re-code diagnoses and assign groups. For this study we are not looking at MCI/ADD subtypes
    ad_datasets_df["diagnosis"] = ad_datasets_df["diagnosis"].replace(
        {"ADD(M)": "ADD", "EMCI": "MCI", "LMCI": "MCI"}
    )
    ad_datasets_df["group"] = ad_datasets_df.apply(assign_mbi_group, axis=1)

    # Save Alzheimer datasets with MBI data
    out_p = root_p / "outputs/ad_datasets_mbi_df.tsv"
    ad_datasets_df.to_csv(out_p, sep="\t", index=False)

    print(f"Saved ad_datasets_df to {out_p}")

    # Drop MBI columns, not needed for further analysis
    ad_datasets_df = ad_datasets_df.drop(
        [
            "decreased_motivation",
            "emotional_dysregulation",
            "impulse_dyscontrol",
            "social_inappropriateness",
            "abnormal_perception",
            "mbi_total_score",
            "mbi_status",
        ],
        axis=1,
    )

    return ad_datasets_df


def create_final_sz_df(qc_pheno_df):
    # Select schizophrenia datasets from qc_pheno
    sz_datasets_df = qc_pheno_df[
        qc_pheno_df["dataset"].isin(["hcpep", "cobre", "srpbs", "ds000030"])
    ].copy()

    # Re-code diagnosis and assign groups
    sz_datasets_df["diagnosis"] = sz_datasets_df["diagnosis"].replace("PSYC", "SCHZ")
    sz_datasets_df["group"] = sz_datasets_df.apply(assign_sz_group, axis=1)

    return sz_datasets_df


if __name__ == "__main__":
    root_p = Path("/home/neuromod/wrangling-phenotype")

    # Load passed_qc_master once
    qc_pheno_p = root_p / "outputs/passed_qc_master.tsv"
    qc_pheno_df = pd.read_csv(qc_pheno_p, sep="\t", low_memory=False)

    # Create dfs for different diagnosis datasets
    ad_datasets_df = create_final_ad_df(qc_pheno_df)
    sz_datasets_df = create_final_sz_df(qc_pheno_df)

    # Concatenate the two sets of datasets
    concat_qc_pheno_df = pd.concat(
        [ad_datasets_df, sz_datasets_df],
        ignore_index=True,
    )

    # Save output
    out_p = root_p / "outputs/final_qc_pheno.tsv"
    concat_qc_pheno_df.to_csv(out_p, sep="\t", index=False)

    print(f"Saved final_qc_pheno_df to {out_p}")
