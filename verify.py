import argparse
from pathlib import Path

from tqdm import tqdm

import createDataset


def sha256(file: Path) -> str:
    import hashlib
    with open(file, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=Path, required=True)
    parser.add_argument('--skip', type=int, default=0)
    parser.add_argument('--path', type=Path, required=False, default=None)
    args = parser.parse_args()
    directory = Path(args.dir)
    skip = args.skip

    tracks = createDataset.load_tracks()
    print("Verifying dataset in", directory.absolute())

    if args.path is not None:
        print("Verifying tracks from", args.path.absolute())
        path = args.path
        lines = open("missing.txt").readlines()
        ids = set()
        for line in lines:
            ids.add(int(line.split("/")[1].split(".")[0]))
        tracks = [t for t in tracks if t.id in ids]

    if skip > 0:
        print("Skipping the first", skip, "tracks")

    tracks = tracks[skip:]

    missing = []
    wrong_sha256 = []

    for t in tqdm(tracks):
        try:
            shouldBePath = directory / f"{t.gdrive_nr}" / f"{t.id}.mp3"
            if not shouldBePath.exists():
                missing.append(t)
                continue

            if t.mp3_sha256 != sha256(shouldBePath):
                wrong_sha256.append(t)
                continue
        except Exception as e:
            print(t.path, e)
            missing.append(t)
            continue

    print(f"Missing ({len(missing)})", [t.path for t in missing])
    print("Wrong sha256 ({len(wrong_sha256)})", [t.path for t in wrong_sha256])
    print("Correct", len(tracks) - len(missing) - len(wrong_sha256))
