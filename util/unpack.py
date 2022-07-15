"""
Extract all .tar files in --dir into the --out directory
"""

import argparse
import tarfile
from pathlib import Path

from tqdm import tqdm

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--dir', type=Path, required=True)
  parser.add_argument('--out', type=Path, required=True)
  args = parser.parse_args()

  for tar_file in tqdm(args.dir.glob("*.tar")):
    with tarfile.open(tar_file) as tar:
      for member in tar.getmembers():
        tar.extract(member, args.out)

  print("Done")
