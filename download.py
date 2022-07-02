from typing import List

import gdown

from track import load_tracks, Track
from pathlib import Path
import argparse


def download_gdrive_files(tracks: List[Track], download_directory: Path):
    gids = set([t.gdrive_id for t in tracks])

    print(f"Found {len(tracks)} tracks")
    failed_gids = []
    for gid in gids:
        print(f"Downloading {gid}")
        if (download_directory / gid).exists():
            print(f"{gid} already downloaded")
            continue
        gdown.download(id=gid, output=str(download_directory / gid))
        if (download_directory / gid).exists():
            print(f"{gid} downloaded")
        else:
            print(f"{gid} download failed")
            failed_gids.append(gid)
            continue

    print(f"Failed to download {len(failed_gids)} files")
    for gid in failed_gids:
        print(f"{gid}")


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument("--mp3", type=Path, default=Path("/tmp/mtg_jamendo_mp3"))
    args = args.parse_args()
    tracks = load_tracks()

    mp3_dir = args.mp3

    gids = set([t.gdrive_id for t in tracks])

    print(f"Found {len(tracks)} tracks")
    failed_gids = []
    for gid in gids:
        print(f"Downloading {gid}")
        if (mp3_dir / gid).exists():
            print(f"{gid} already downloaded")
            continue
        gdown.download(id=gid, output=str(mp3_dir / gid))
        if (mp3_dir / gid).exists():
            print(f"{gid} downloaded")
        else:
            print(f"{gid} download failed")
            failed_gids.append(gid)
            continue

    print(f"Failed to download {len(failed_gids)} files")
    for gid in failed_gids:
        print(f"{gid}")
