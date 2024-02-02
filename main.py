"""Load uk biobank data and extract demographic information.

Author: Hao-Ting Wang; last edit 2024-02-02

All input stored in `data/` folder. The content of `data` is not
included in the repository.

Labels are cureated from the ukbb website and saved to `data/`.
This is public information hence it is not included in the repository.

data_file is provided by SJ's lab and requires DUA,
hence it is not included in the repository.

To expand this tool, add more  information to `info` with information
from Data_fields xlsx file (provided by SJ's lab and requires DUA).

"""
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
                'name': 'init',
                'description': 'first imaging visit',
            },
            3: {
                'name': 'repeat',
                'description': 'first repeat imaging visit',
            }
        }
    }
}


def read_ukbb_data(data_file, info_label):

    if info_label not in info:
        raise ValueError(
            f'Unsupported data for extraction: {info_label}. Available data: '
            f'{list(info.keys())}'
        )

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
            names.append(
                f'{info_label}_{v["name"]}'
            )
    else:
        fids.append(f'f.{info[info_label]["id"]}.0.0')
        descriptions.append(info[info_label]['description'])
        names.append(info_label)

    if 'labels' in info[info_label]:
        labels = pd.read_csv(
            f'data/{info[info_label]["labels"]}', sep='\t', index_col=0
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
    return data, meta_data


if __name__ == '__main__':
    # use argparse to get datafile
    parser = argparse.ArgumentParser(description='Extract ukbb data')
    parser.add_argument('datafile', type=str, help='ukbb data file')
    args = parser.parse_args()
    data_file = args.datafile

    curated_data = []
    curated_meta = {}
    for i in info:
        data, meta_data = read_ukbb_data(data_file, i)
        curated_data.append(data)
        curated_meta.update(meta_data)
    curated_data = pd.concat(curated_data, axis=1)

    curated_data.to_csv('outputs/ukbb_pheno.tsv', sep='\t')
    with open('outputs/ukbb_pheno.json', 'w') as f:
        json.dump(curated_meta, f, indent=2)