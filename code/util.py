def calculate_mbi_score(df):
    mbi_domains = [
        "decreased_motivation",
        "emotional_dysregulation",
        "impulse_dyscontrol",
        "social_inappropriateness",
        "abnormal_perception",
    ]

    df["mbi_total_score"] = df[mbi_domains].sum(axis=1)
    df["mbi_status"] = (df["mbi_total_score"] >= 1).astype(int)
    return df
