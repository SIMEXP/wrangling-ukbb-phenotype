import pandas as pd
import argparse
import h5py

from pathlib import Path


def count_frames(hdf5_object, target_suffix="atlas-MIST_desc-64_timeseries", path=""):
    data = []
    for name in hdf5_object:
        item_path = f"{path}/{name}" if path else name
        if isinstance(hdf5_object[name], h5py.Dataset) and name.endswith(target_suffix):
            # Collect dataset name and the first dimension of its shape
            data.append(
                {"file": name, "total_frames": hdf5_object[name].shape[0]}
            )  # n_frames
        elif isinstance(hdf5_object[name], h5py.Group):
            # Recursive call to navigate through groups and collect data
            data.extend(count_frames(hdf5_object[name], target_suffix, item_path))
    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a .tsv listing number of frames remaining for speicific atlas/strategy"
    )
    parser.add_argument(
        "input_p",
        type=Path,
        help="Path to directory containing subject directories [participant] or .h5 file of connectomes [group]",
    )
    parser.add_argument("output_p", type=Path, help="Output path")

    parser.add_argument(
        "level",
        choices=["group", "participant"],
        help="Analysis level: 'group' or 'participant'",
    )

    parser.add_argument("dataset", type=str, help="Name of the dataset")

    args = parser.parse_args()

    data = []
    if args.level == "participant":
        for sub_directory in args.input_p.iterdir():
            if sub_directory.is_dir() and sub_directory.name != "working_directory":
                hdf5_file_p = (
                    sub_directory
                    / f"sub-{sub_directory.name}_atlas-MIST_desc-simple.h5"  # atlas-MIST_desc-scrubbing.5+gsr.h5"
                )
                if hdf5_file_p.exists():
                    with h5py.File(hdf5_file_p, "r") as file:
                        data.extend(count_frames(file))
                else:
                    print(f"File {hdf5_file_p} does not exist.")
    elif args.level == "group":
        hdf5_file_p = (
            args.input_p / "atlas-MIST_desc-simple.h5"
        )  # "atlas-MIST_desc-scrubbing.5+gsr.h5"
        with h5py.File(hdf5_file_p, "r") as file:
            data = count_frames(file)

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(data)

    # Extract file details. Session and run are optional
    pattern = (
        r"sub-([^\s/_]+)(?:_ses-([a-zA-Z0-9]+))?_task-([a-zA-Z0-9]+)(?:_run-(\d+))?"
    )
    df[["participant_id", "ses", "task", "run"]] = df["file"].str.extract(pattern)
    df["participant_id"] = df["participant_id"].astype(str)

    # Drop the file column and add dataset as a column
    df.drop(columns=["file"], inplace=True)
    df["dataset"] = args.dataset

    # Save output
    # df.to_csv(args.output_p / f"{args.dataset}_frames.tsv", sep="\t", index=False)
    df.to_csv(args.output_p / f"{args.dataset}_frames_total.tsv", sep="\t", index=False)
