"""
Downloads the original mp3 files from https://github.com/MTG/mtg-jamendo-dataset and
converts them to opus format. The final files are stored under ./opus/
"""
import os
import subprocess
import tarfile
from collections import defaultdict
from pathlib import Path
from typing import Set, Iterable, Dict, Any, Callable, List
import pandas as pd
from dataclasses import dataclass
import gdown
import argparse
import multiprocessing as mp
import logging

from tqdm import tqdm

def download_gdrive(gid: str, download_directory: Path) -> Path:
    """Download gdrive file under /tmp/mtg_jamendo_opus/<gid>"""
    download_directory.mkdir(exist_ok=True, parents=True)
    gdown_file = download_directory / gid
    if not gdown_file.exists():
        gdown.download(id=gid, output=str(gdown_file.absolute()))
    return gdown_file


def leadingZerosTwo(n: int) -> str:
    return f"{n:0>2}"


@dataclass(frozen=True)
class Track:
    id: int
    artist_id: int
    album_id: int
    durationInSec: float
    genres: Set[str]
    instruments: Set[str]
    moods: Set[str]
    gdrive_nr: int
    gdrive_id: str
    gdrive_size_in_gb: float
    main_genre: str
    main_instrument: str
    main_mood: str
    chunk_nr: int

    def mp3_path(self, target_directory: Path):
        return target_directory / leadingZerosTwo(self.gdrive_nr) / f"{self.id}.mp3"

    def download(self, mp3_directory: Path):
        """Downloads the gdrive file and extract the track"""
        if self.mp3_path(mp3_directory).exists():
            print("Already downloaded", self.mp3_path(mp3_directory).absolute())
            return
        gdrive_path = download_gdrive(self.gdrive_id, mp3_directory)
        try:
            with tarfile.open(gdrive_path) as tar:
                print("Extract", gdrive_path.name)
                # open your tar.gz file
                for member in tqdm(iterable=tar.getmembers(), total=len(tar.getmembers())):
                    # Extract member
                    tar.extract(member=member, path=mp3_directory)
        except FileNotFoundError:
            print("File not found", gdrive_path.absolute())
            raise FileNotFoundError
        print("Delete", gdrive_path.name)
        gdrive_path.unlink()

    def convert(self, mp3_directory: Path, target_directory: Path, ffmpeg_path: Path, kbits=64):
        """Converts the mp3 file to opus"""
        mp3_path = self.mp3_path(mp3_directory)
        opus_path = target_directory / f"{self.id}.opus"
        if opus_path.exists():
            print("Already converted", opus_path.absolute())
            return
        print("Convert", mp3_path.name)
        subprocess.run(
            [ffmpeg_path, '-y', '-i', mp3_path.absolute().__str__(), '-c:a', 'libopus',
             '-b:a',
             f'{kbits}k', opus_path.absolute().__str__()], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def groupedBy(iterable: Iterable[Track], key_func: Callable[[Track], Any]) -> Dict:
    ret = defaultdict(list)
    for k in iterable:
        ret[key_func(k)].append(k)
    return dict(ret)


parser = argparse.ArgumentParser()
parser.add_argument("--mp3", type=Path, default=Path("/tmp/mtg_jamendo_mp3"))
parser.add_argument("--opus", type=Path, default=Path("./opus"))
parser.add_argument("--kb", type=int, default=64, help="opus bitrate in kbits")
parser.add_argument("--ffmpeg", type=Path, default=Path("ffmpeg"), help="Path to ffmpeg")
parser.add_argument("--cpus", type=int, default=1, help="Number of spawned processes")


def process_tracks(tx: List[Track], mp3: Path, opus: Path, ffmpeg: Path, kbit: int):
    for i, t in enumerate(tx):
        try:
            t.download(mp3)
            t.convert(mp3, opus, ffmpeg, kbit)
            print(f"[{os.getpid()}] Done with track:{t.id} | {len(tx) - i} remaining")
        except Exception as e:
            print(f"[{os.getpid()}] Error with track:{t.id} | {len(tx) - i} remaining")




if __name__ == '__main__':
    args = parser.parse_args()

    df = pd.read_parquet("tracks.parquet")
    tracks = [Track(**row) for row in df.to_dict(orient="records")]
    print(f"Found {len(tracks)} tracks")

    mp3_dir = args.mp3
    opus_dir = args.opus
    ffmpeg_path = args.ffmpeg
    kbits = args.kb
    n_cpus = args.cpus

    gid_groups = groupedBy(tracks, lambda t: t.gdrive_nr % n_cpus)
    processes = []
    for group_nr, tracks in gid_groups.items():
        p = mp.Process(target=process_tracks, args=(tracks, mp3_dir, opus_dir, ffmpeg_path, kbits))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

    # for chunk_nr, chunk_tracks in groupedBy(tracks, "chunk_nr").items():
    #     create_archive(chunk_nr, chunk_tracks)
    #     delete_tracks(chunk_tracks)
    #
    # tracks[0].download(Path("."))
