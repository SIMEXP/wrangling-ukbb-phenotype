"""Load SRPBS (Open) data and extract demographic information.

Author: Natasha Clarke; last edit 2024-02-14

All input stored in `data/srpbs` folder. The content of `data` is not
included in the repository.

The data participants.tsv is downloaded from https://bicr-resource.atr.jp/srpbsopen/. The data is not public, hence it is not included in this repository. See "HOW TO DOWNLOAD THE DATA"
for access instructions.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "participant_id",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "sex",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "site",
        "description": "Site of imaging data collection",
        "levels": {
            "SWA": "Showa University",
            "HUH": "Hiroshima University Hospital",
            "HRC": "Hiroshima Rehabilitation Center",
            "HKH": "Hiroshima Kajikawa Hospital",
            "COI": "Hiroshima COI",
            "KUT": "Kyoto University TimTrio",
            "KTT": "Kyoto University Trio",
            "UTO": "University of Tokyo Hospital",
            "ATT": "ATR Trio",
            "ATV": "ATR Verio",
            "CIN": "CiNet",
            "NKN": "Nishinomiya Kyouritsu Hospital",
        },
    },
    "diagnosis": {
        "original_field_name": "diag",
        "description": "Diagnosis of the participant",
        "levels": {
            "CON": "control",
            "ASD": "autism spectrum disorder",
            "MDD": "major depressive disorder",
            "OCD": "obsessive compulsive disorder",
            "SCHZ": "schizophrenia",
            "PAIN": "pain",
            "STROKE": "stroke",
            "BIPOLAR": "bipolar disorder",
            "DYSTHYMIA": "dysthmia",
            "OTHER": "other",
        },
    },
}


def process_data(csv_file_p, output_p, metadata):
    # Load the CSV
    df = pd.read_csv(csv_file_p, sep="\t")

    # Remove sub- from participant id
    df["participant_id"] = df["participant_id"].str.replace("sub-", "", regex=False)

    # Process the data
    df["age"] = df["age"].astype(float)
    df["sex"] = df["sex"].map({2: "female", 1: "male"})
    df["diagnosis"] = df["diag"].map(
        {
            0: "CON",
            1: "ASD",
            2: "MDD",
            3: "OCD",
            4: "SCHZ",
            5: "PAIN",
            6: "STROKE",
            7: "BIPOLAR",
            8: "DYSTHYMIA",
            99: "OTHER",
        }
    )

    # Select columns
    df = df[["participant_id", "age", "sex", "site", "diagnosis"]]

    # Output tsv file
    df.to_csv(output_p / "srpbs_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "srpbs_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Data and metadata have been processed and output to {output_p}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process SRPBS (Open) phenotype data and output to TSV and JSON"
    )
    parser.add_argument("datafile", type=Path, help="Path to the input TSV data file")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.datafile, args.output, metadata)
