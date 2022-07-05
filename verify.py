import argparse
from pathlib import Path
import tarfile
import pandas as pd
from tqdm import tqdm

from createDataset import load_tracks

"""
given a directory of tar files with the name "raw_30s_audio-<id>.tar"
where <id> is the id of the tar file

1. extract every tar file
    - this will create a directory with the name "<id>" and multiple mp3 files inside


"""


def extract_tar_file(tarFile: Path):
    print("Extract", tarFile.name)
    with tarfile.open(tarFile) as tar:
        for member in tqdm(iterable=tar.getmembers(), total=len(tar.getmembers())):
            if (directory / member.name).exists():
                continue
            tar.extract(member=member, path=directory)


def sha256(file: Path) -> str:
    import hashlib
    with open(file, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=Path, required=True)
    args = parser.parse_args()
    directory = Path(args.dir)

    tracks = load_tracks()
    print("Verifying dataset in", directory.absolute())

    missing = []
    wrong_sha256 = []

    for t in tqdm(tracks):
        shouldBePath = directory / f"{t.gdrive_nr}" / f"{t.id}.mp3"
        if not shouldBePath.exists():
            missing.append(t)
            continue

        if t.mp3_sha256 != sha256(shouldBePath):
            wrong_sha256.append(t)
            continue

    print("Missing", len(missing))
    print("Wrong sha256", len(wrong_sha256))
    print("Correct", len(tracks) - len(missing) - len(wrong_sha256))
