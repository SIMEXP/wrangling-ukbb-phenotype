import pandas as pd
import numpy as np
from pathlib import Path


def map_values(df):
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
    # Calculate MBI domains
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
