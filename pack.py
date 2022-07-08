import argparse
from pathlib import Path
import tarfile

from tqdm import tqdm

from createDataset import load_tracks

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--opus', type=Path, required=True, help="Directory containing opus files")
  parser.add_argument('--to', type=Path, required=True, help="Directory to put the packed files in")
  parser.add_argument('--filter', type=str, default=None, help="Filter tracks by expression")
  args = parser.parse_args()

  tracks = load_tracks()
  if args.filter:
    predicate = lambda t: eval(args.filter)(t)
    tracks = [t for t in tracks if predicate(t)]
  track_ids = set(t.id for t in tracks)

  groups = {}
  for track_file in tqdm(args.opus.glob("*.opus"), desc="Creating groups"):
    track_id = int(track_file.stem)
    if track_id not in track_ids:
      continue
    track = next(t for t in tracks if t.id == track_id)
    group = track.chunk_nr
    if group not in groups:
      groups[group] = []
    groups[group].append(track)

  print("Packing", sum([len(g) for g in groups.values()]), "tracks")
  print("Into", len(groups), "groups")

  args.to.mkdir(parents=True, exist_ok=True)

  for group, group_tracks in tqdm(groups.items(), desc="Packing"):
    # open tar file
    tar_path = Path(args.to) / f"{group}.tar"
    with tarfile.open(tar_path, "w") as tar:
      for track in group_tracks:
        # add opus file
        opus_path = args.opus / f"{track.id}.opus"
        tar.add(opus_path, arcname=f"{track.id}.opus")

  print("Done")
