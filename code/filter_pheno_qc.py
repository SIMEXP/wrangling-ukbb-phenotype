"""Load phenotypic data for N datasets, filter according to scans that passed QC on specific column, and output a merged tsv with pheno and QC results.

To run, needs:
- rest_df.tsv, a TSV file of concatenated QC results, generated using qc_output/qc.ipynb
- TSV of pheno data for each specificed dataset, generated using the corresponding script in this repo

Author: Natasha Clarke; last edit 2024-02-29

"""

import argparse
import pandas as pd
from pathlib import Path


def filter_pheno_qc(qc_df, pheno_df, which_qc_col, dataset):
    pheno_df["participant_id"] = pheno_df["participant_id"].astype(str)
    qc_df["participant_id"] = qc_df["participant_id"].astype(str)
    qc_df_filtered = qc_df[qc_df["dataset"] == dataset]

    merged_df = qc_df_filtered.merge(pheno_df, on="participant_id", how="left")

    # Handle site columns
    merged_df.drop(columns=["site_x"], inplace=True)
    merged_df.rename(columns={"site_y": "site"}, inplace=True)

    # Filter for passed QC
    filtered_df = merged_df[merged_df[which_qc_col] == True]

    # Select relevant columns
    filtered_df = filtered_df[
        [
            "identifier",
            "participant_id",
            "ses",
            "run",
            "age",
            "sex",
            "site",
            "proportion_kept",
            "mean_fd_raw",
            "diagnosis",
            "dataset",
        ]
    ]

    return filtered_df


def create_master(rest_qc_p, datasets, which_qc_col):
    # Load QC df once
    qc_df = pd.read_csv(rest_qc_p, sep="\t")

    # Process each dataset and create a master of scans which passed QC
    master_df = pd.DataFrame()
    for dataset in datasets:
        pheno_p = Path(root_p / pheno_p_template.format(dataset=dataset))
        pheno_df = pd.read_csv(pheno_p, sep="\t", dtype={"participant_id": str})
        df = filter_pheno_qc(qc_df, pheno_df, which_qc_col, dataset)
        master_df = pd.concat([master_df, df], ignore_index=True)

    return master_df


def filter_diagnoses(df, diagnoses):
    return df[df["diagnosis"].isin(diagnoses)]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter phenotype data according to QC result"
    )
    parser.add_argument("--root", type=str, help="Root path for data")
    parser.add_argument(
        "--datasets", nargs="+", type=str, help="List of datasets to process"
    )
    parser.add_argument(
        "--which_qc_col", type=str, help="Column for filtering QC. Default=pass_func_qc"
    )
    parser.add_argument(
        "--diagnoses", nargs="+", type=str, help="Diagnoses of interest"
    )

    args = parser.parse_args()
    datasets = args.datasets
    which_qc_col = args.which_qc_col or "pass_func_qc"
    diagnoses = args.diagnoses if args.diagnoses else None

    root_p = Path(args.root)
    pheno_p_template = "wrangling-phenotype/outputs/{dataset}_pheno.tsv"
    rest_qc_p = Path(root_p / "qc_output/rest_df.tsv")
    output_p = Path(root_p / "wrangling-phenotype/outputs")

    master_df = create_master(rest_qc_p, datasets, which_qc_col)
    sub_df = filter_diagnoses(master_df, diagnoses)

    if diagnoses:
        sub_df.to_csv(output_p / "passed_qc_master.tsv", sep="\t", index=False)
    else:
        master_df.to_csv(output_p / "passed_qc_master.tsv", sep="\t", index=False)

    print(f"Data have been processed and output to {output_p}")
