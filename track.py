import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Set, List

import pandas as pd


def load_tracks() -> List["Track"]:
    df = pd.read_parquet("tracks.parquet")
    tracks = [Track(**row) for row in df.to_dict(orient="records")]
    return tracks


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

    # def download(self, mp3_directory: Path):
    #     """Downloads the gdrive file and extract the track"""
    #     if self.mp3_path(mp3_directory).exists():
    #         print("Already downloaded", self.mp3_path(mp3_directory).absolute())
    #         return
    #     gdrive_path = download_gdrive(self.gdrive_id, mp3_directory)
    #     try:
    #         with tarfile.open(gdrive_path) as tar:
    #             print("Extract", gdrive_path.name)
    #             # open your tar.gz file
    #             for member in tqdm(iterable=tar.getmembers(), total=len(tar.getmembers())):
    #                 # Extract member
    #                 tar.extract(member=member, path=mp3_directory)
    #     except FileNotFoundError:
    #         print("File not found", gdrive_path.absolute())
    #         raise FileNotFoundError
    #     print("Delete", gdrive_path.name)
    #     gdrive_path.unlink()

    def convert(self, mp3_directory: Path, target_directory: Path, ffmpeg_path: Path, kbits=64):
        """Converts the mp3 file to opus"""
        mp3_path = self.mp3_path(mp3_directory)
        target_directory.mkdir(exist_ok=True, parents=True)
        opus_path = target_directory / f"{self.id}.opus"
        if opus_path.exists():
            print("Already converted", opus_path.absolute())
            return
        print("Convert", mp3_path, "to", opus_path)
        subprocess.run(
            [ffmpeg_path, '-y', '-i', mp3_path.absolute().__str__(), '-c:a', 'libopus',
             '-b:a',
             f'{kbits}k', opus_path.absolute().__str__()], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def leadingZerosTwo(n: int) -> str:
    return f"{n:0>2}"
