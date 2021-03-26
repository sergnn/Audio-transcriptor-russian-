import argparse
import hashlib
from pathlib import Path
from subprocess import run
from typing import List


def hash_dir(file_path: Path) -> str:
    return hashlib.md5(str(file_path.parent).encode('UTF-8')).hexdigest()


def prepare_files_list(input_dir, file_mask='*'):
    """Scan directory for files"""
    files_list = []
    suffix = Path(file_mask).suffix.lower()

    for sub in Path(input_dir).glob('*'):
        if sub.is_dir():
            yield from prepare_files_list(sub, file_mask)

        if sub.is_file() and (file_mask == '*' or sub.suffix.lower() == suffix):
            if str(sub.stem) == hash_dir(sub):
                continue
            files_list.append(sub)

    if files_list:
        yield files_list


def prepare_ffmpeg_list(files_list: List[Path]) -> str:
    """Prepare ffmpeg input list file"""
    output_str = ''
    for filename in files_list:
        output_str += f"file '{filename}'\n"
    return output_str


def concatenate(files_list: List[Path], ffmpeg_path='ffmpeg', remove=False):
    """Run ffmpeg to concatenate files"""
    current_dir = files_list[0].parent
    print(f'\nProcessing {current_dir}')
    ffmpeg_list = current_dir / 'files_to_concat.txt'
    ffmpeg_list.write_text(prepare_ffmpeg_list(files_list), encoding='utf8')
    output_file = current_dir / (hash_dir(files_list[0]) + files_list[0].suffix)
    cmd = [str(ffmpeg_path), '-y', '-f', 'concat', '-safe', '0', '-i', str(ffmpeg_list), '-c', 'copy', str(output_file)]

    p = run(cmd, shell=True)
    if p.returncode:
        print('Something went wrong')
    else:
        print('Successfully concatenated')

    try:
        ffmpeg_list.unlink()
    except Exception:
        print(f'Unable to remove {ffmpeg_list}')

    if remove:
        for file in files_list:
            try:
                file.unlink()
            except Exception:
                print(f'Unable to remove {file}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir', help="Input dir")
    parser.add_argument("--ext", default='*', help="Move only files with extension")
    parser.add_argument("--ffmpeg", default='ffmpeg', help="Path to ffmpeg")
    parser.add_argument("--remove", action='store_true', help="Remove input files")

    args = parser.parse_args()

    for files in prepare_files_list(args.input_dir, args.ext):
        concatenate(files, args.ffmpeg, args.remove)
