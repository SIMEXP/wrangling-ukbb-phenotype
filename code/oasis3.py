"""Load OASIS3 data and extract demographic information.

Author: Natasha Clarke; last edit 2024-03-01

All input stored in `data/oasis3` folder. The content of `data` is not
included in the repository.

The data clarken_8_28_2023_13_54_33.csv is downloaded from XNAT Central. The data is not public, hence it is not included in this repository. See https://www.oasis-brains.org/#data for Data Usage Agreement and how to get the data.
for access instructions.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "Subject",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "Age is included, however some fields were missing and age was rounded to the nearest year. To determine a more accurate age, we calculate it using ageAtEntry and the days from entry included in the field MR ID",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "M/F",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "None given - in the case of single site study, the site name is the dataset name",
        "description": "Site of imaging data collection",
    },
    "diagnosis": {
        "original_field_name": "We will determine diagnosis using a number of fields",
        "description": "Diagnosis of the participant",
        "levels": "to be confirmed",
    },
}


def process_diagnosis_data(df):
    df["diagnosis"] = None

    df.loc[
        (df["NORMCOG"] == 1)
        | (
            (df["dx1"] == "Cognitively normal")
            & (~df["diagnosis"].isin(["MCI", "DEMENTED"]))
        ),
        "diagnosis",
    ] = "CON"
    mci_columns = [
        "MCIAMEM",
        "MCIAPLUS",
        "MCIAPLAN",
        "MCIAPATT",
        "MCIAPEX",
        "MCIAPVIS",
        "MCINON1",
        "MCIN1LAN",
        "MCIN1ATT",
        "MCIN1EX",
        "MCIN1VIS",
        "MCINON2",
        "MCIN2LAN",
        "MCIN2ATT",
        "MCIN2EX",
        "MCIN2VIS",
    ]
    df.loc[df[mci_columns].sum(axis=1) > 0, "diagnosis"] = "MCI"
    df.loc[(df["PROBADIF"] == 1), "diagnosis"] = "ADD"
    df.loc[(df["POSSADIF"] == 1), "diagnosis"] = "ADD(POSS)"
    df.loc[(df["DLBIF"] == 1), "diagnosis"] = "DLB"
    df.loc[(df["VASCIF"] == 1), "diagnosis"] = "VASC"
    df.loc[(df["VASCPSIF"] == 1), "diagnosis"] = "VASC(POSS)"
    df.loc[(df["ALCDEMIF"] == 1), "diagnosis"] = "ALCDEM"
    df.loc[(df["DEMUNIF"] == 1), "diagnosis"] = "DEMUN"
    df.loc[(df["FTDIF"] == 1), "diagnosis"] = "FTD"
    df.loc[(df["PPAPHIF"] == 1), "diagnosis"] = "PPA"
    df.loc[(df["PSPIF"] == 1), "diagnosis"] = "PSP"
    df.loc[(df["CORTIF"] == 1), "diagnosis"] = "CORT"
    df.loc[(df["HUNTIF"] == 1), "diagnosis"] = "HUNT"
    df.loc[(df["PRIONIF"] == 1), "diagnosis"] = "PRION"
    df.loc[(df["MEDSIF"] == 1), "diagnosis"] = "MEDS"
    df.loc[(df["DYSILLIF"] == 1), "diagnosis"] = "DYSILL"
    df.loc[(df["DEPIF"] == 1), "diagnosis"] = "DEP"
    df.loc[(df["OTHPSYIF"] == 1), "diagnosis"] = "OTHPSY"
    df.loc[(df["DOWNSIF"] == 1), "diagnosis"] = "DOWNS"
    df.loc[(df["PARKIF"] == 1), "diagnosis"] = "PARK"
    df.loc[(df["STROKIF"] == 1), "diagnosis"] = "STROKE"
    df.loc[(df["HYCEPHIF"] == 1), "diagnosis"] = "HYCEPH"
    df.loc[(df["BRNINJIF"] == 1), "diagnosis"] = "TBI"
    df.loc[(df["NEOPIF"] == 1), "diagnosis"] = "NEOP"
    df.loc[
        (df["COGOTHIF"] == 1) | (df["COGOTH2F"] == 1) | (df["COGOTH3F"] == 1),
        "diagnosis",
    ] = "COGOTH"

    # Codes get a bit unreliable from here! Mopping up some codes:
    df.loc[
        ((df["dx1"] == "uncertain dementia") & df["diagnosis"].isna()),
        "diagnosis",
    ] = "DEMUN"

    df.loc[
        (
            (df["dx1"] == "DAT")
            | (df["dx1"] == "AD Dementia")
            | (df["dx1"] == "AD dem w/CVD contribut")
            | (df["dx1"] == "AD dem w/CVD not contrib")
            | (df["dx1"] == "AD dem w/depresss- not contribut")
        )
        & (df["diagnosis"].isna()),
        "diagnosis",
    ] = "ADD"

    df.loc[
        (df["dx1"] == "No dementia")
        & (df["cdr"] == 0)
        & pd.isna(df["WHODIDDX"])
        & df["diagnosis"].isna(),
        "diagnosis",
    ] = "CON"

    df.loc[
        (
            (df["dx1"] == "Vascular Demt- primary")
            | (df["dx1"] == "Vascular Demt  primary")
        )
        & pd.isna(df["WHODIDDX"])
        & df["diagnosis"].isna(),
        "diagnosis",
    ] = "VASC"

    df.loc[
        ((df["dx1"] == "Non AD dem- Other primary") & df["diagnosis"].isna()),
        "diagnosis",
    ] = "COGOTH"

    df.loc[
        ((df["dx1"] == "uncertain- possible NON AD dem") & df["diagnosis"].isna()),
        "diagnosis",
    ] = "DEMUN"

    df.loc[
        (
            (df["dx1"] == "Unc: ques. Impairment")
            | (df["dx1"] == "Incipient demt PTP")
            | (df["dx1"] == "Unc: impair reversible")
            | (df["dx1"] == "0.5 in memory only")
        )
        & (df["diagnosis"].isna()),
        "diagnosis",
    ] = "UNCERT"

    return df


def process_data(csv_file_p, output_p, metadata):
    # Load the CSV
    df = pd.read_csv(csv_file_p)

    # Drop rows that don't pertain to participants
    df = df.loc[~df["MR ID"].str.contains("OASIS3_data_files|OASIS_cohort_files")]

    # Determine diagnoses based on columns
    df = process_diagnosis_data(df)

    # Process the data
    df["participant_id"] = df["Subject"]
    # Age: extract the number of days from the 'MR ID', calculate age at time of scan
    df["daysFromEntry"] = df["MR ID"].str.extract("d(\d+)").astype(int)
    df["age"] = (
        (df["ageAtEntry"] + df["daysFromEntry"] / 365.25).astype(float).round(2)
    )  # Or round to 1?

    df["sex"] = df["M/F"].map({"F": "female", "M": "male"})
    df["site"] = "oasis3"  # There is only one site, and no name provided

    # Select columns
    # df = df[["participant_id", "age", "sex", "site", "diagnosis"]]

    # Output tsv file
    df.to_csv(output_p / "oasis3_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "oasis3_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process OASIS3 phenotype data and output to TSV and JSON"
    )
    parser.add_argument("datafile", type=Path, help="Path to the input TSV data file")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.datafile, args.output, metadata)
