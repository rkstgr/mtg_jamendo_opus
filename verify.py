import argparse
from itertools import repeat
from pathlib import Path

from parallelbar import progress_map
from tqdm import tqdm

import createDataset


def sha256(file: Path) -> str:
    import hashlib
    try:
        with open(file, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except Exception as e:
        print(e)
        return ""


def verify_track(root_directory, track: createDataset.Track):
    mp3_path = root_directory / track.path
    if not mp3_path.exists():
        print("Missing mp3", mp3_path.absolute())
        return False
    if sha256(mp3_path) != track.mp3_sha256:
        print("Wrong sha256", mp3_path.absolute())
        return False
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=Path, required=True)
    parser.add_argument('--filter', type=str, default=None)
    args = parser.parse_args()
    directory = Path(args.dir)

    tracks = createDataset.load_tracks()

    if args.filter:
        predicate = lambda t: eval(args.filter)(t)
        tracks = [t for t in tracks if predicate(t)]

    print("Verifying dataset in", directory.absolute())
    print("Verifying", len(tracks), "tracks")

    tasks = zip(repeat(directory), tracks)
    result = progress_map(verify_track, tasks)

    with open("missing-tasks.txt", "w") as f:
        for i, r in enumerate(result):
            if not r:
                f.write(f"{tracks[i].id}\n")

    print("Correct hash:", len([r for r in result if r]))
    print("Missing or error:", len([r for r in result if not r]))
