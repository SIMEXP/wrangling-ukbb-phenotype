"""Load phenotypic and QC data for datasets, summarise QC pass rate, and output a .tsv with passed QC scans in order to obtain connectomes.
Optionally, pass specific diagnoses.

To run, needs:
- TSV of pheno and qc data for each specificed dataset, generated using the corresponding script in this repo

Author: Natasha Clarke; last edit 2024-07-16

"""

import argparse
import pandas as pd
from pathlib import Path


def create_master_df(root_p, datasets):
    # Create one df of QC results for specified datasets
    cols = [
        "identifier",
        "participant_id",
        "ses",
        "run",
        "age",
        "sex",
        "site",
        "diagnosis",
        "dataset",
        "mean_fd_raw",
        "mean_fd_scrubbed",
        "proportion_kept",
        "functional_dice",
        "pass_func_qc",
        "anatomical_dice",
        "pass_anat_qc",
        "pass_all_qc",
        "different_func_affine",
    ]  # scanner

    master_df = pd.DataFrame()
    for dataset in datasets:
        path_template = "wrangling-phenotype/outputs/{dataset}_qc_pheno.tsv"
        df_p = root_p / path_template.format(dataset=dataset)
        df = pd.read_csv(df_p, sep="\t", dtype={"participant_id": str})
        df = df[cols]
        master_df = pd.concat([master_df, df], ignore_index=True)

    return master_df


def filter_diagnoses(df, diagnoses):
    return df[df["diagnosis"].isin(diagnoses)]


def summarise_qc(df):
    # Across all scans i.e. rows
    scan_summary = (
        master_df.groupby("dataset")
        .agg(
            total_scans=("participant_id", "count"),  # Count all scans per dataset
            total_scan_passes=(which_qc_col, "sum"),  # Sum passes per dataset
        )
        .reset_index()
    )

    # Across sessions i.e. only count one run per session
    session_passes = (
        master_df.groupby(["dataset", "participant_id", "ses"])[which_qc_col]
        .any()
        .reset_index()
    )
    session_summary = session_passes.groupby("dataset").agg(
        total_sessions=("ses", "count"),  # Count unique sessions per dataset
        total_session_passes=(which_qc_col, "sum"),  # Count sessions that passed
    )

    # Across participants i.e. only count one session per participant
    subject_summary = (
        session_passes.groupby(["dataset", "participant_id"])[which_qc_col]
        .any()
        .reset_index()
    )
    subject_summary = subject_summary.groupby("dataset").agg(
        total_subjects=("participant_id", "count"),  # Count unique subjects per dataset
        total_subject_passes=(which_qc_col, "sum"),  # Count subjects that passed
    )

    return (scan_summary, session_summary, subject_summary)


def combine_summaries(scan_summary, session_summary, subject_summary):
    # Ensure 'dataset' is a column for joining
    session_summary = session_summary.reset_index()
    subject_summary = subject_summary.reset_index()

    # Merge summaries
    combined_summary = pd.merge(
        scan_summary, session_summary, on="dataset", how="outer"
    )
    combined_summary = pd.merge(
        combined_summary, subject_summary, on="dataset", how="outer"
    )

    combined_summary.columns = [
        "dataset",
        "Total Scans",
        "Total Scan Passes",
        "Total Sessions",
        "Total Session Passes",
        "Total Subjects",
        "Total Subject Passes",
    ]

    return combined_summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate QC summary metrics and .tsv of scans passed QC"
    )
    parser.add_argument("--root_p", type=Path, help="Root path for data")
    parser.add_argument(
        "--datasets", nargs="+", type=str, help="List of datasets to process"
    )
    parser.add_argument(
        "--which_qc_col",
        type=str,
        help="Column for evaluating QC. Default=pass_func_qc",
    )
    parser.add_argument(
        "--diagnoses", nargs="+", type=str, help="Diagnoses of interest"
    )

    args = parser.parse_args()
    datasets = args.datasets
    which_qc_col = args.which_qc_col or "pass_func_qc"
    diagnoses = args.diagnoses if args.diagnoses else None
    output_p = args.root_p / "wrangling-phenotype/outputs"

    # Create df of QC results and pheno data for specified datasets
    master_df = create_master_df(args.root_p, datasets)

    # Optionally filter df for specific diagnoses
    if diagnoses:
        master_df = filter_diagnoses(master_df, diagnoses)

    # Summarise QC results
    # Fill in blank session labels with a dummy variable to enable aggregation
    master_df["ses"] = master_df["ses"].fillna("dummy").replace("", "dummy")
    scan_summary, session_summary, subject_summary = summarise_qc(master_df)
    qc_summary = combine_summaries(scan_summary, session_summary, subject_summary)
    # Remove dummy variable
    master_df["ses"].replace("dummy", "", inplace=True)

    # Filter for scans that passed QC based on specified column (default = pass_func_qc)
    filtered_df = master_df[master_df[which_qc_col] == True].copy()

    # Save output
    qc_summary.to_csv(output_p / "qc_summary.tsv", sep="\t", index=False)
    filtered_df.to_csv(output_p / "passed_qc_master.tsv", sep="\t", index=False)

    print(f"Data have been processed and output to {output_p}")
