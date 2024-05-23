"""Load uk biobank data and extract demographic information.

Author: Hao-Ting Wang; last edit 2024-02-02

All input stored in `data/ukbb` folder. The content of `data` is not
included in the repository.

Labels are cureated from the ukbb website and saved to `data/ukbb`.
This is public information hence it is not included in the repository.
You will need to download coding9, coding10, and coding19.
For example, coding9 can be downloaded from:
https://biobank.ndph.ox.ac.uk/ukb/coding.cgi?id=9

data_file is provided by SJ's lab and requires DUA,
hence it is not included in the repository.

To expand this tool, add more  information to `info` with information
from Data_fields xlsx file (provided by SJ's lab and requires DUA).

"""
from pathlib import Path
import pandas as pd
import json
import argparse

# phenotypic information curated through ukbb website
info = {
    'sex': {
        'id': 31,
        'description': 'Sex',
        'labels': 'coding9.tsv',  # download from ukbb website
    },
    'age':{
        'id': 21022,
        'description': 'Age at recruitment (year)',
    },
    'site':{
        'id': 54,
        'description': 'Assessment centre',
        'labels': 'coding10.tsv',  # download from ukbb website
        'instance':{  # curated from ukbb website
            2: {
                'name': '',
                'description': 'first imaging visit',
            },
            3: {
                'name': 'repeat',
                'description': 'first repeat imaging visit',
            }
        }
    },
    'diagnosis':{
        'id': 41270,
        'description': 'Diagnoses - International Classification of Disease version 10 (ICD10)',
        'labels': 'coding19.tsv',  # download from ukbb website
        'instance':{
            'F00': {
                'label': 'ADD',
                'description': 'Alzheimer disease - Dementia',
            },
            'F10': {
                'label': 'ALCO',
                'description': 'Alcohol Abuse',
            },
            'F20': {
                'label': 'SCZ',
                'description': 'Schizophrenia',
            },
            'F31': {
                'label': 'BIPOLAR',
                'description': 'Bipolar disorder',
            },
            'F32': {
                'label': 'DEP',
                'description': 'Depressive disorder',
            },
            'G20': {
                'label': 'PARK',
                'description': 'Parkinson',
            },
            'G30': {
                'label': 'ADD',
                'description': 'Alzheimer disease - Dementia',
            },
            'G35': {
                'label': 'MS',
                'description': 'Multiple sclerosis',
            },
            'G40': {
                'label': 'EPIL',
                'description': 'Epilepsy',
            },
        }
    }
}


def read_ukbb_diagnosis(data_file):
    # read data_file with subject id and all columns started with f.41270
    data = pd.read_csv(data_file, sep='\t', na_values='NA', index_col=0, low_memory=False)
    data = data.filter(regex='^f.41270', axis=1)
    for idx, row in data.iterrows():
        row = row.dropna()
        # reduce the value to the first three characters
        row = row.apply(lambda x: x[:3])
        row = row.tolist()
        if len(row) > 0:
            # assign as the default as control
            data.loc[idx, 'icd10'] = row[0]
            data.loc[idx, 'n_icd10'] = len(row)
            data.loc[idx, 'diagnosis'] = 'CON'
            # take the first value that matches diagnosis of interest
            while row:
                label = row.pop(0)
                if label in info['diagnosis']['instance']:
                    data.loc[idx, 'diagnosis'] = info['diagnosis']['instance'][label]['label']
                    data.loc[idx, 'icd10'] = label
                    break
        else:
            # no history of diagnosis at all wow
            data.loc[idx, 'n_icd10'] = 0
            data.loc[idx, 'icd10'] = None
            data.loc[idx, 'diagnosis'] = 'HC'
                
    meta_data = {
        'diagnosis': {
            'fid': 'f.41270.x',
            'description': info['diagnosis']['description'],
            'labels': {  # curated from ukbb website
                'ADD': 'Alzheimer disease - Dementia',
                'ALCO': 'Alcohol Abuse',
                'SCZ': 'Schizophrenia',
                'BIPOLAR': 'Bipolar disorder',
                'PARK': 'Parkinson',
                'MS': 'Multiple sclerosis',
                'EPIL': 'Epilepsy',
                'CON': 'Control',
                'HC': 'Healthy control',
            }
        }
    }
    return data[['diagnosis', 'icd10', 'n_icd10']], meta_data


def read_ukbb_data(data_file, info_label):

    if info_label not in info:
        raise ValueError(
            f'Unsupported data for extraction: {info_label}. Available data: '
            f'{list(info.keys())}'
        )

    # diagnostic data has to be handled differently
    if info_label == 'diagnosis':
        return read_ukbb_diagnosis(data_file)

    # compile meta data and construct id in the datafile (fid)
    # format: f.<id>.<instance>.0
    # not sure the field for 0 is for, but it is always 0
    meta_data = {}
    labels = None
    fids = []
    descriptions = []
    names = []

    if 'instance' in info[info_label]:
        for k, v in info[info_label]['instance'].items():
            fids.append(f'f.{info[info_label]["id"]}.{k}.0')
            descriptions.append(
                f'{info[info_label]["description"]}; {v["description"]}'
            )
            if v['name']:
                name = f'{info_label}_{v["name"]}'
            else:
                name = info_label
            names.append(
                name
            )
    else:
        fids.append(f'f.{info[info_label]["id"]}.0.0')
        descriptions.append(info[info_label]['description'])
        names.append(info_label)

    if 'labels' in info[info_label]:
        labels = pd.read_csv(
            f'data/ukbb/{info[info_label]["labels"]}', sep='\t', index_col=0
        )
        labels = labels.to_dict()['meaning']

    for f, d, n in zip(fids, descriptions, names):
        meta_data[n] = {
            'fid': f,
            'description': d,
        }
        if labels:
            meta_data[n]['labels'] = labels

    # read data from the giant tsv file
    columns = ['f.eid'] + list(v['fid'] for v in meta_data.values())
    data = pd.read_csv(
        data_file, sep='\t', na_values='NA', index_col=0, usecols=columns)
    data.columns = names
    data.index.name = 'participant_id'
    return data, meta_data


if __name__ == '__main__':
    # use argparse to get datafile
    parser = argparse.ArgumentParser(description='Extract ukbb data')
    parser.add_argument('datafile', type=Path, help='ukbb data file')
    parser.add_argument('output', type=Path, help='output directory')

    args = parser.parse_args()
    data_file = args.datafile
    output_dir = args.output

    curated_data = []
    curated_meta = {}
    for i in info:
        data, meta_data = read_ukbb_data(data_file, i)
        curated_data.append(data)
        curated_meta.update(meta_data)
    curated_data = pd.concat(curated_data, axis=1)

    curated_data.to_csv(output_dir / 'ukbb_pheno.tsv', sep='\t')
    with open(output_dir / 'ukbb_pheno.json', 'w') as f:
        json.dump(curated_meta, f, indent=2)
