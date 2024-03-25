"""Load phenotypic data for N datasets, summarise QC pass rate, and output a .tsv with passed QC scans in order to obtain connectomes. Optionally, pass specific diagnoses but note that the summary is currently calculated on the whole dataset.


filter according to scans that passed QC on specific column, and output a merged tsv with pheno and QC results.

To run, needs:
- rest_df.tsv, a TSV file of concatenated QC results, generated using qc_output/qc.ipynb
- TSV of pheno data for each specificed dataset, generated using the corresponding script in this repo

Author: Natasha Clarke; last edit 2024-03-12

"""

import argparse
import pandas as pd
from pathlib import Path


def merge_adni(qc_df_filtered, pheno_df):
    qc_df_filtered["ses"] = pd.to_datetime(qc_df_filtered["ses"])
    qc_df_filtered = qc_df_filtered.sort_values(by="ses")

    pheno_df["ses"] = pd.to_datetime(pheno_df["ses"])
    pheno_df = pheno_df.sort_values(by="ses")

    merged_df = pd.merge_asof(
        qc_df_filtered,
        pheno_df,
        by="participant_id",  # Match participants
        on="ses",  # Find the nearest match based on session date
        direction="nearest",
        tolerance=pd.Timedelta(days=183),
    )

    return merged_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter phenotype data according to QC result"
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

    rest_qc_p = args.root_p / "qc_output/rest_df.tsv"
    frames_p = args.root_p / "wrangling-phenotype/data/frames/total_frames_master.tsv"
    output_p = args.root_p / "wrangling-phenotype/outputs"

    # Load QC and frames data
    qc_df = pd.read_csv(
        rest_qc_p, sep="\t", low_memory=False
    )  # dtype={"participant_id": str, "ses": str}
    frames_df = pd.read_csv(frames_p, sep="\t", dtype={"participant_id": str})

    # Merge pheno and qc data for each dataset
    master_df = pd.DataFrame()
    for dataset in datasets:
        pheno_p_template = "wrangling-phenotype/outputs/{dataset}_pheno.tsv"
        pheno_p = args.root_p / pheno_p_template.format(dataset=dataset)
        pheno_df = pd.read_csv(pheno_p, sep="\t", dtype={"participant_id": str})

        qc_df_filtered = qc_df[qc_df["dataset"] == dataset].copy()
        if dataset == "adni":
            df = merge_adni(qc_df_filtered, pheno_df)

    # Save output
    # qc_summary_df.to_csv(output_p / "qc_summary.tsv", sep="\t", index=False)
    # matched_df.to_csv(output_p / "passed_qc_master.tsv", sep="\t", index=False)
    df.to_csv(output_p / "test.tsv", sep="\t", index=False)

    print(f"Data have been processed and output to {output_p}")
