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


def merge_pheno_qc(qc_df, pheno_df, which_qc_col, dataset):
    """
    Merges QC data with phenotype data for a specific dataset.

    Parameters:
    - qc_df (pd.DataFrame): DataFrame containing QC data, must include 'participant_id' and 'dataset' columns.
    - pheno_df (pd.DataFrame): DataFrame containing phenotype data, must include 'participant_id'.
    - which_qc_col (str): Name of the QC column to indicate QC status.
    - dataset (str): Name of the dataset to filter the merged DataFrame on.

    Returns:
    - pd.DataFrame: A DataFrame merged on 'participant_id', filtered by 'dataset',
      with relevant columns selected, including the specified QC status column.
    """
    pheno_df["participant_id"] = pheno_df["participant_id"].astype(str)
    qc_df["participant_id"] = qc_df["participant_id"].astype(str)
    qc_df_filtered = qc_df[qc_df["dataset"] == dataset]

    merged_df = qc_df_filtered.merge(pheno_df, on="participant_id", how="left")

    # Handle site columns
    merged_df.drop(columns=["site_x"], inplace=True)
    merged_df.rename(columns={"site_y": "site"}, inplace=True)

    # Select relevant columns
    merged_df = merged_df[
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
        + [which_qc_col]
    ]

    return merged_df


def create_master_df(root_p, qc_df, datasets, which_qc_col):
    """
    Creates a master DataFrame by merging QC data with phenotype data across multiple datasets.

    Parameters:
    - qc_df (pd.DataFrame): DataFrame containing QC data, including a column for QC status and 'participant_id'.
    - datasets (list of str): List containing the names of the datasets to process.
    - which_qc_col (str): Name of the QC column to indicate QC status.

    Returns:
    - pd.DataFrame: A master DataFrame containing merged QC and phenotype data for all specified datasets.
                     The DataFrame includes an additional column specified by which_qc_col to indicate the
                     QC status of each scan.

    Note:
    - Assumes that there is a phenotypic df available for each dataset.
    """
    master_df = pd.DataFrame()
    for dataset in datasets:
        pheno_p_template = "wrangling-phenotype/outputs/{dataset}_pheno.tsv"
        pheno_p = args.root_p / pheno_p_template.format(dataset=dataset)
        pheno_df = pd.read_csv(pheno_p, sep="\t", dtype={"participant_id": str})
        df = merge_pheno_qc(qc_df, pheno_df, which_qc_col, dataset)
        master_df = pd.concat([master_df, df], ignore_index=True)

    return master_df


def filter_diagnoses(df, diagnoses):
    """
    Filters a DataFrame to retain only rows that match specified diagnoses.

    Parameters:
    - df (pd.DataFrame): DataFrame to be filtered, expected to include a 'diagnosis' column.
    - diagnoses (list of str): List of diagnoses to filter by. Rows with a 'diagnosis' value
                               matching any item in this list will be retained.

    Returns:
    - pd.DataFrame: A filtered DataFrame containing only the rows that match one of the specified diagnoses.
    """
    return df[df["diagnosis"].isin(diagnoses)]


def summarise_sessions(dataset_df, which_qc_col):
    """
    Summarises session pass rates based on the specified QC column. For a given session, it is counted as one pass if any run passed QC.

    Parameters:
    - dataset_df (pd.DataFrame): DataFrame containing session data. Must include
                                 columns for 'participant_id', 'ses' (session), and the specified QC column.
    - which_qc_col (str): Name of the QC column to indicate QC status.

    Returns:
    - tuple:
        - pd.DataFrame: DataFrame of unique sessions with an additional column 'session_passed' indicating
                        if any run in the session passed QC.
        - int: The total number of unique sessions.
        - int: The number of unique sessions that passed QC.
        - float: The percentage of unique sessions that passed QC.
    """

    # Fill in blank session labels with a default value
    dataset_df["ses"] = dataset_df["ses"].fillna("ses1").replace("", "ses1")

    # Calculate if any run in a session passed QC
    session_passed = dataset_df.groupby(["participant_id", "ses"])[
        which_qc_col
    ].transform(lambda x: any(x))
    dataset_df.loc[:, "session_passed"] = session_passed

    # Drop duplicates to ensure each session is counted once
    unique_sessions_df = dataset_df.drop_duplicates(subset=["participant_id", "ses"])

    # Count total and passed sessions
    total_sessions = len(unique_sessions_df)
    passed_sessions = len(
        unique_sessions_df[unique_sessions_df["session_passed"] == True]
    )

    # Calculate percentage of sessions that passed
    if total_sessions > 0:
        percentage_passed = (passed_sessions / total_sessions) * 100
    else:
        percentage_passed = 0

    # Replace the dummy session variable
    unique_sessions_df = unique_sessions_df.copy()
    unique_sessions_df["ses"] = unique_sessions_df["ses"].replace("ses1", "")

    return unique_sessions_df, total_sessions, passed_sessions, percentage_passed


def summarise_subjects(dataset_df, unique_sessions_df):
    """
    Summarises the QC pass rates for unique subjects based on session data. For a given subject, they are counted as one pass if any session passed QC.

    Parameters:
    - dataset_df (pd.DataFrame): DataFrame containing subject data. Must include a
                                 'participant_id' column.
    - unique_sessions_df (pd.DataFrame): DataFrame of session data, including a 'session_passed'
                                         column indicating if any run in the session passed QC.

    Returns:
    - tuple:
        - int: The total number of unique subjects in dataset_df.
        - int: The number of unique subjects with at least one session that passed QC.
        - float: The percentage of unique subjects that passed QC based on session data.
    """
    # Total unique subjects
    total_unique_subjects = dataset_df["participant_id"].nunique()

    # Identify unique subjects that passed any session
    passed_subjects_df = unique_sessions_df[
        unique_sessions_df["session_passed"] == True
    ]
    unique_passed_subjects = passed_subjects_df["participant_id"].nunique()

    # Calculate percentage of unique subjects that passed
    if total_unique_subjects > 0:
        percentage_unique_subjects_passed = (
            unique_passed_subjects / total_unique_subjects
        ) * 100
    else:
        percentage_unique_subjects_passed = 0

    return (
        total_unique_subjects,
        unique_passed_subjects,
        percentage_unique_subjects_passed,
    )


def summarise_passed_qc(merged_df, dataset, which_qc_col):
    """
    Generates a summary DataFrame indicating QC pass rates for both sessions and subjects within a specific dataset.

    Parameters:
    - merged_df (pd.DataFrame): Merged DataFrame containing both QC and phenotype data.
    - dataset (str): Name of the dataset to filter for.
    - which_qc_col (str): Name of the QC column to indicate QC status.

    Returns:
    - pd.DataFrame: A DataFrame containing a summary of QC pass rates, including the total number of sessions, the number of sessions that passed QC, the percentage of sessions that passed, the total number of subjects, the number of subjects that passed QC, and the percentage of subjects that passed QC for the specified dataset.
    """
    # Filter for the specified dataset
    dataset_df = merged_df[merged_df["dataset"] == dataset].copy()

    # Summarise unique session passes
    unique_sessions_df, total_sessions, passed_sessions, percentage_passed = (
        summarise_sessions(dataset_df, which_qc_col)
    )
    # Summarise unique subject passes
    total_unique_subjects, unique_passed_subjects, percentage_unique_subjects_passed = (
        summarise_subjects(dataset_df, unique_sessions_df)
    )

    # Create a summary df
    qc_summary_df = pd.DataFrame(
        [
            {
                "dataset": dataset,
                "qc_column": which_qc_col,
                "total_sessions": total_sessions,
                "sessions_passed_qc (any 1 run)": passed_sessions,
                "percentage_sessions_passed_qc": round(percentage_passed),
                "total_subjects": total_unique_subjects,
                "subjects_passed_qc (any 1 session)": unique_passed_subjects,
                "percentage_subjects_passed_qc": round(
                    percentage_unique_subjects_passed
                ),
            }
        ]
    )

    return qc_summary_df


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
    qc_df = pd.read_csv(rest_qc_p, sep="\t")
    frames_df = pd.read_csv(frames_p, sep="\t", dtype={"participant_id": str})

    # Create df of pheno and qc results
    master_df = create_master_df(args.root_p, qc_df, datasets, which_qc_col)

    # Summarise QC results
    qc_summary_df_list = []
    for dataset in datasets:
        qc_summary_df = summarise_passed_qc(master_df, dataset, which_qc_col)
        qc_summary_df_list.append(qc_summary_df)

    qc_summary_df = pd.concat(qc_summary_df_list, ignore_index=True)

    # Filter df for scans that passed QC only, optionally for specific diagnoses
    if diagnoses:
        sub_df = filter_diagnoses(master_df, diagnoses)
        filtered_df = sub_df[sub_df[which_qc_col] == True].copy()
    else:
        filtered_df = master_df[master_df[which_qc_col] == True].copy()

    # Match QC df with frames df (total frames or number of frames remaining)
    frames_df = frames_df.loc[frames_df["task"] == "rest"].copy()
    matched_df = filtered_df.merge(
        frames_df, on=["participant_id", "ses", "run", "dataset"], how="inner"
    )

    # Save output
    qc_summary_df.to_csv(output_p / "qc_summary.tsv", sep="\t", index=False)
    matched_df.to_csv(output_p / "passed_qc_master.tsv", sep="\t", index=False)

    print(f"Data have been processed and output to {output_p}")
