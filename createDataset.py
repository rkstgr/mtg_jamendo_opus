"""
Downloads the original mp3 files from https://github.com/MTG/mtg-jamendo-dataset and
converts them to opus format. The final files are stored under ./opus/
"""
import os
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Dict, Any, Callable, List
import argparse
import multiprocessing as mp

from track import load_tracks, Track


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

    tracks = load_tracks()
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
