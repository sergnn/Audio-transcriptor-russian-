import argparse
import os
import uuid
from pathlib import Path


def flatten_dir(input_dir: Path, output_dir: Path, extension: str, use_hash: bool):
    """
    Flatten dir
    root/file1.txt              → output/parent_dir_root/file1.txt
    root/dir1/file1.txt         → output/dir1/file1.txt
    root/dir1/file2.txt         → output/dir1/file2.txt
    root/dir1/dir1_1/file1.txt  → output/dir1/dir1_1_file2.txt
    root/dir2/file1.txt         → output/dir2/file1.txt
    root/dir2/dir2_1/file1.txt  → output/dir2/dir2_1_file2.txt
    root/dir2/dir2_2/file1.txt  → output/dir2/dir2_2_file2.txt
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    parent_cache = {}
    for file in input_dir.rglob(extension):
        if file.is_dir():
            continue

        relative_path = file.relative_to(input_dir)

        try:
            parent = relative_path.parent.parts[0]
            first_level_dir_relative = file.relative_to(input_dir / parent)
        except IndexError:
            parent = 'parent_dir_root'
            first_level_dir_relative = file.relative_to(input_dir)

        if use_hash:
            new_filename = f'{uuid.uuid4().hex}{file.suffix}'

            if parent not in parent_cache:
                parent_cache[parent] = uuid.uuid4().hex
            new_path = output_dir / parent_cache[parent] / new_filename
        else:
            new_filename = str(first_level_dir_relative).replace(os.path.sep, '_')
            new_path = output_dir / parent / new_filename

        print(file, '→', new_path)
        new_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            file.rename(new_path)
        except FileNotFoundError:
            """Probaly path too long (>260 chars)"""
            print('Too long path, processing...')
            file = Path(u'\\\\?\\' + str(file.absolute()))
            new_path = Path(u'\\\\?\\' + str(new_path.absolute()))
            file.rename(new_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir', help="Input dir")
    parser.add_argument('output_dir', help="Output dir")
    parser.add_argument("--ext", default='*', help="Move only files with extension")
    parser.add_argument("--hash", help="Use hash instead of filenames", action="store_true")
    args = parser.parse_args()
    flatten_dir(Path(args.input_dir), Path(args.output_dir), args.ext, args.hash)
