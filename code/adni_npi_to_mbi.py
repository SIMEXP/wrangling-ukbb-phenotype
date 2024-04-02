import pandas as pd
import numpy as np
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


# set paths
adnimerge_p = Path(
    "/home/neuromod/wrangling-phenotype/data/adni/ADNIMERGE_22Aug2023.csv"
)
npi_p = Path("/home/neuromod/wrangling-phenotype/data/adni/NPI_22Aug2023.csv")
qc_pheno_p = Path("/home/neuromod/wrangling-phenotype/outputs/passed_qc_master.tsv")
output_p = Path("/home/neuromod/wrangling-phenotype/test.tsv")

# load data
adnimerge_df = pd.read_csv(adnimerge_p, low_memory=False)
npi_df = pd.read_csv(npi_p)
qc_pheno_df = pd.read_csv(qc_pheno_p, sep="\t")

# convert NPI to MBI and calculate total score
mbi_df = adni_npi_to_mbi(npi_df)
mbi_df = util.calculate_mbi_score(mbi_df)
mbi_df = select_columns(mbi_df)

# Rename date field
mbi_df.rename(columns={"EXAMDATE": "ses"}, inplace=True)

# In qc_pheno_df grab just the ID part from participant_id, so it matches mbi_df
qc_df_filtered = qc_pheno_df.loc[qc_pheno_df["dataset"] == "adni"].copy()
qc_df_filtered["RID"] = (
    qc_df_filtered["participant_id"].str.split("S").str[-1].astype(int)
)

# Replace some rougue dates
mbi_df["ses"] = mbi_df["ses"].replace("0012-02-14", "2012-02-14")
mbi_df["ses"] = mbi_df["ses"].replace("0013-05-06", "2013-05-06")
mbi_df["ses"] = mbi_df["ses"].replace("0013-10-28", "2013-10-28")

# Convert sessions to datetime
qc_df_filtered["ses"] = pd.to_datetime(qc_df_filtered["ses"])
mbi_df["ses"] = pd.to_datetime(mbi_df["ses"])

# Ensure ordered by ses
qc_df_filtered = qc_df_filtered.sort_values(by=["ses"])
mbi_df = mbi_df.dropna(subset=["ses"])  # Since some were missing
mbi_df = mbi_df.sort_values(by=["ses"])

# Merge to get nearest mbi result within 6 months
merged_df = pd.merge_asof(
    qc_df_filtered,
    mbi_df,
    by="RID",
    on="ses",
    direction="nearest",
    tolerance=pd.Timedelta(days=183),  # Approximately 6 months
)

# TO DO: how best to output? Now have adni filtered from qc_pheno
merged_df.to_csv(output_p, sep="\t", index=False)
