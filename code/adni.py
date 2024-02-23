"""Load ADNI data and extract demographic information.

Author: Natasha Clarke; last edit 2024-02-23

All input stored in `data/adni` folder. The content of `data` is not
included in the repository.

The data is downloaded from https://adni.loni.usc.edu/. The data is not public, hence it is not included in this repository. See https://adni.loni.usc.edu/data-samples/access-data/
for access instructions. Note: adni_spreadsheet.csv was handered over to me by Desiree, who was given it by Hanad. It lists the scanning data, but I am using a spreadsheet downloaded from ADNI directly for diagnoses.

"""

import pandas as pd
import json
import argparse
from pathlib import Path

# Define metadata
metadata = {
    "participant_id": {
        "original_field_name": "Subject ID in adni_spreadsheet.csv (PTID in ADNIMERGE_22Aug2023.csv)",
        "description": "Unique identifier for each participant",
    },
    "age": {
        "original_field_name": "Age",
        "description": "Age of the participant in years",
    },
    "sex": {
        "original_field_name": "Sex",
        "description": "Sex of the participant",
        "levels": {"male": "male", "female": "female"},
    },
    "site": {
        "original_field_name": "Site is provided in the subject ID, e.g. for 011_S_0002 the site is 11",
        "description": "Site of imaging data collection",
        "levels": "unable to find the matching site names. Acquisition sites can be found here: https://adni.loni.usc.edu/about/centers-cores/study-sites/",
    },
    "diagnosis": {
        "original_field_name": "",
        "description": "Diagnosis of the participant",
        "levels": {},
    },
}


def process_data(scan_file_p, diagnosis_file_p, output_p, metadata):
    # Load the CSVs
    df = pd.read_csv(scan_file_p, index_col=0)
    diagnosis_df = pd.read_csv(diagnosis_file_p, low_memory=False)

    # Process the data
    df["age"] = df["Age"].astype(float)
    df["sex"] = df["Sex"].map({"F": "female", "M": "male"})
    df["site"] = (
        df["Subject ID"].str.split("_").str[0].str.lstrip("0")
    )  # Extract site variable from ID and remove leading zeros
    df["participant_id"] = df["Subject ID"].str.replace("_", "", regex=False)

    # Select columns
    df = df[["participant_id", "age", "sex", "site"]]  # Diagnosis to be added

    # Sort df
    df = df.sort_values(by="participant_id")

    # Output tsv file
    df.to_csv(output_p / "adni_pheno.tsv", sep="\t", index=False)

    # Output metadata to json
    with open(output_p / "adni_pheno.json", "w") as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process ADNI phenotype data and output to to TSV and JSON"
    )
    parser.add_argument("scanfile", type=Path, help="Path to adni_spreadsheet.csv")
    parser.add_argument("diagfile", type=Path, help="Path to ADNIMERGE_22Aug2023.csv")
    parser.add_argument("output", type=Path, help="Path to the output directory")

    args = parser.parse_args()

    process_data(args.scanfile, args.diagfile, args.output, metadata)
