"""
Downloads the original mp3 files from https://github.com/MTG/mtg-jamendo-dataset and
converts them to opus format. The final files are stored under ./opus/
"""
import subprocess
import tarfile
from collections import defaultdict
from pathlib import Path
from typing import Set, Iterable
import pandas as pd
from dataclasses import dataclass
import gdown
import argparse

tmp_directory = Path("/tmp/mtg_jamendo_opus")


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
            return
        gdrive_path = download_gdrive(self.gdrive_id, mp3_directory)
        with tarfile.open(gdrive_path) as tar:
            print("Extract", gdrive_path.name)
            tar.extractall(mp3_directory)  # extract like /mp3_directory/<gdrive_nr>/<id>.mp3
        print("Delete", gdrive_path.name)
        gdrive_path.unlink()

    def convert(self, mp3_directory: Path, target_directory: Path, ffmpeg_path: Path, kbits=64):
        """Converts the mp3 file to opus"""
        mp3_path = self.mp3_path(mp3_directory)
        opus_path = target_directory / f"{self.id}.opus"
        if opus_path.exists():
            return
        print("Convert", mp3_path.name)
        subprocess.run(
            [ffmpeg_path, '-y', '-i', mp3_path.absolute().__str__(), '-c:a', 'libopus',
             '-b:a',
             f'{kbits}k', opus_path.absolute().__str__()], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def groupedBy(iterable: Iterable[Track], attribute: str):
    ret = defaultdict(list)
    for k in iterable:
        ret[k.__getattribute__(attribute)].append(k)
    return dict(ret)


parser = argparse.ArgumentParser()
parser.add_argument("--mp3_directory", type=Path, default=tmp_directory)
parser.add_argument("--opus_directory", type=Path, default=Path("./opus"))
parser.add_argument("--kbits", type=int, default=64)
parser.add_argument("--ffmpeg_path", type=Path, default=Path("ffmpeg"))

if __name__ == '__main__':
    args = parser.parse_args()

    df = pd.read_parquet("tracks.pqt")
    tracks = [Track(**row) for row in df.to_dict(orient="records")]

    mp3_dir = args.mp3_directory
    opus_dir = args.opus_directory
    ffmpeg_path = args.ffmpeg_path
    kbits = args.kbits

    for t in tracks[0:1]:
        t.download(mp3_dir)
        t.convert(mp3_directory=mp3_dir, target_directory=opus_dir, ffmpeg_path=ffmpeg_path, kbits=kbits)

    # for chunk_nr, chunk_tracks in groupedBy(tracks, "chunk_nr").items():
    #     create_archive(chunk_nr, chunk_tracks)
    #     delete_tracks(chunk_tracks)
    #
    # tracks[0].download(Path("."))
