import numpy as np


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
