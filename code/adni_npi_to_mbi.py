import pandas as pd
import numpy as np
import argparse
import util

from pathlib import Path


def adni_npi_to_mbi(df):
    df["decreased_motivation"] = df["NPIG"]
    df["emotional_dysregulation"] = df["NPID"] + df["NPIE"] + df["NPIF"]
    df["impulse_dyscontrol"] = df["NPIC"] + df["NPII"] + df["NPIJ"]
    df["social_inappropriateness"] = df["NPIH"]
    df["abnormal_perception"] = df["NPIA"] + df["NPIB"]
    return df


def select_columns(df):
    columns = [
        "RID",
        "EXAMDATE",
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


def adni_merge_mbi_qc(qc_pheno_df, mbi_df):
    # Grab just the ID part from participant_id, so it matches mbi_df
    qc_pheno_df["RID"] = (
        qc_pheno_df["participant_id"].str.split("S").str[-1].astype(int)
    )

    # Rename date field so it matches
    mbi_df.rename(columns={"EXAMDATE": "ses"}, inplace=True)

    # Replace some rougue dates
    mbi_df["ses"] = mbi_df["ses"].replace("0012-02-14", "2012-02-14")
    mbi_df["ses"] = mbi_df["ses"].replace("0013-05-06", "2013-05-06")
    mbi_df["ses"] = mbi_df["ses"].replace("0013-10-28", "2013-10-28")

    # Convert sessions to datetime
    qc_pheno_df["ses"] = pd.to_datetime(qc_pheno_df["ses"])
    mbi_df["ses"] = pd.to_datetime(mbi_df["ses"])

    # Ensure ordered by session
    qc_pheno_df = qc_pheno_df.sort_values(by=["ses"])
    mbi_df = mbi_df.dropna(subset=["ses"])  # Since some were missing
    mbi_df = mbi_df.sort_values(by=["ses"])

    # Merge to get nearest mbi result within 6 months
    merged_df = pd.merge_asof(
        qc_pheno_df,
        mbi_df,
        by="RID",
        on="ses",
        direction="nearest",
        tolerance=pd.Timedelta(days=183),
    )

    return merged_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert NPI to MBI, merge with QC and pheno data for ADNI"
    )
    parser.add_argument("rootpath", type=Path, help="Root path")

    args = parser.parse_args()
    root_p = args.rootpath

    # Set paths
    npi_p = root_p / "data/adni/NPI_22Aug2023.csv"
    qc_pheno_p = root_p / "outputs/passed_qc_master.tsv"
    output_p = root_p / "outputs/final_adni.tsv"

    # Load data
    npi_df = pd.read_csv(npi_p)
    qc_pheno_df = pd.read_csv(qc_pheno_p, sep="\t", low_memory=False)

    # Filter for dataset
    qc_pheno_df = qc_pheno_df[qc_pheno_df["dataset"] == "adni"]

    # Convert NPI to MBI and calculate total score
    mbi_df = adni_npi_to_mbi(npi_df)
    mbi_df = util.calculate_mbi_score(mbi_df)
    mbi_df = select_columns(mbi_df)

    merged_df = adni_merge_mbi_qc(qc_pheno_df, mbi_df)

    # Select scans. For controls, we take the first available scan. For MCI and ADD, take the first with an MBI score
    final_adni = (
        merged_df.groupby(["participant_id"], as_index=False)
        .apply(util.select_row)
        .reset_index(drop=True)
    )

    # Output df
    final_adni.to_csv(output_p, sep="\t", index=False)
