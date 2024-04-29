import numpy as np
import pandas as pd


def calculate_mbi_score(df):
    mbi_domains = [
        "decreased_motivation",
        "emotional_dysregulation",
        "impulse_dyscontrol",
        "social_inappropriateness",
        "abnormal_perception",
    ]

    # Calculate mbi_total_score across domains
    df["mbi_total_score"] = df[mbi_domains].sum(axis=1, min_count=1)

    # Set mbi_total_score to NaN where all mbi_domain columns are NaN
    df.loc[df[mbi_domains].isna().all(axis=1), "mbi_total_score"] = np.nan

    # Calculate mbi_status based on mbi_total_score
    # Set mbi_status to NaN where mbi_total_score is NaN
    df["mbi_status"] = np.where(
        df["mbi_total_score"].isna(), np.nan, (df["mbi_total_score"] >= 1).astype(int)
    )

    return df


def select_row(group):
    if group["diagnosis"].iloc[0] == "CON":
        # For 'CON', the first row
        return group.head(1)
    else:
        # For 'ADD' and 'MCI', select the first row where 'mbi_status' has a value
        return group[group["mbi_status"].notna()].head(1)


def adni_npi_to_mbi(df):
    df["decreased_motivation"] = df["NPIG"]
    df["emotional_dysregulation"] = df["NPID"] + df["NPIE"] + df["NPIF"]
    df["impulse_dyscontrol"] = df["NPIC"] + df["NPII"] + df["NPIJ"]
    df["social_inappropriateness"] = df["NPIH"]
    df["abnormal_perception"] = df["NPIA"] + df["NPIB"]
    return df


def adni_select_columns(df):
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
    qc_pheno_df = qc_pheno_df.copy()
    mbi_df = mbi_df.copy()

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


def cimaq_map_values(df):
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


def cimaq_select_columns(df):
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
    qc_pheno_df = qc_pheno_df.copy()
    mbi_df = mbi_df.copy()

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


def oasis3_npi_to_mbi(df):
    df["decreased_motivation"] = df["APA"]
    df["emotional_dysregulation"] = df["DEPD"] + df["ANX"] + df["ELAT"]
    df["impulse_dyscontrol"] = df["AGIT"] + df["IRR"] + df["MOT"]
    df["social_inappropriateness"] = df["DISN"]
    df["abnormal_perception"] = df["DEL"] + df["HALL"]
    return df


def oasis3_select_columns(df):
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


def oasis3_merge_mbi_qc(qc_pheno_df, mbi_df):
    qc_pheno_df = qc_pheno_df.copy()
    mbi_df = mbi_df.copy()

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
