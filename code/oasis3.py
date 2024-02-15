"""Load OASIS3 data and extract demographic information.

Author: Natasha Clarke; last edit 2024-02-15

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


def process_data(csv_file_p, output_p, metadata):
    # Load the CSV
    df = pd.read_csv(csv_file_p)

    # Drop rows that don't pertain to participants
    df = df.loc[~df["MR ID"].str.contains("OASIS3_data_files|OASIS_cohort_files")]

    # Process the data
    df["participant_id"] = df["Subject"]
    # Age: extract the number of days from the 'MR ID', calculate age at time of scan
    df["daysFromEntry"] = df["MR ID"].str.extract("d(\d+)").astype(int)
    df["age"] = (df["ageAtEntry"] + df["daysFromEntry"] / 365.25).astype(float).round(2)

    df["sex"] = df["M/F"].map({"F": "female", "M": "male"})
    df["site"] = "oasis3"  # There is only one site, and no name provided

    # Select columns
    df = df[["participant_id", "age", "sex", "site"]]  # Diagnosis field to be added

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
