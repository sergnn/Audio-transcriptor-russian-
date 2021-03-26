import argparse
import hashlib
from pathlib import Path


def sort_files(input_dir: Path, output_dir: Path):
    """
    Sort files by dir
    dir1/1.txt → hash1_1/1.txt
    dir1/1.wav → hash1_1/1.wav
    dir2/1.txt → hash1_2/1.txt
    dir2/1.wav → hash1_2/1.wav
    dir2/2.txt → hash2_1/2.txt
    dir2/2.wav → hash2_1/2.wav
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    index_cache = {}
    for file in input_dir.rglob('*'):
        if file.is_dir():
            continue
        filename_hash = hashlib.md5(file.stem.encode("UTF-8")).hexdigest()

        if filename_hash not in index_cache:
            index_cache[filename_hash] = 0
        elif (output_dir / f'{filename_hash}_{index_cache[filename_hash]}' / f'{filename_hash}{file.suffix}').exists():
            index_cache[filename_hash] += 1

        new_filepath = output_dir / f'{filename_hash}_{index_cache[filename_hash]}' / f'{filename_hash}{file.suffix}'
        new_filepath.parent.mkdir(parents=True, exist_ok=True)
        print(file, '→', new_filepath)
        file.rename(new_filepath)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir', help="Input dir")
    parser.add_argument('output_dir', help="Output dir")
    args = parser.parse_args()
    sort_files(Path(args.input_dir), Path(args.output_dir))
