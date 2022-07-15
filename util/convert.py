"""
Convert mp3 files to opus files
"""
import argparse
from itertools import repeat
from pathlib import Path
from typing import Tuple, List

import ffmpeg
from parallelbar import progress_map


def convert(tasks: List[Tuple[Path, Path, int]]):
    from_mp3, to_opus, kBits = tasks
    if to_opus.exists():
        print("Skipping", to_opus.absolute(), "(already exists)")
        return

    (
        ffmpeg
        .input(str(from_mp3.absolute()))
        .output(str(to_opus.absolute()), acodec="libopus", audio_bitrate=f"{kBits}k")
        .run(overwrite_output=True, capture_stdout=True, capture_stderr=True)
    )

    if not to_opus.exists():
        print("Conversion failed for ", from_mp3.absolute())


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Convert mp3 files to opus files")
    parser.add_argument("--from", help="Directory containing mp3 files", required=True)
    parser.add_argument("--to", help="Directory to write opus files to", required=True)
    #parser.add_argument("--ffmpeg", help="Path to ffmpeg", default="ffmpeg")
    parser.add_argument("--kbits", default=64, type=int, help="Bitrate of opus file")
    parser.add_argument("--n_cpu", default=1, type=int, help="Number of CPUs to use")
    args = parser.parse_args()

    mp3_directory = Path(args.__getattribute__("from"))
    target_directory = Path(args.to)

    if not mp3_directory.exists():
        raise FileNotFoundError(f"Directory {mp3_directory} does not exist")

    target_directory.mkdir(exist_ok=True, parents=True)

    print("Converting mp3 files in directory", mp3_directory.absolute())

    mp3_files = list(mp3_directory.glob("**/*.mp3"))
    opus_files = [target_directory / f"{f.stem}.opus" for f in mp3_files]
    tasks = list(zip(mp3_files, opus_files, repeat(args.kbits)))

    print("Converting", len(tasks), "files")

    progress_map(convert, tasks, n_cpu=args.n_cpu)
