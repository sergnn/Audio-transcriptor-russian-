# Audio Transcriptor
# This script will create the folder structure in the same manner as LJSpeech-1.1 dataset.
# This script will splitt the audio files on silenses and send the audio chunks to google recognition service
# Google will return the recognized text.
# This text will be writen to metadata.csv in the same manner as in LJSpeech-1.1 dataset.
# The audio chunks will also be saved in the same manner as in LJSpeech-1.1 dataset.

# This script must be in the same folder with audio files that should be transcripted
# The names of the audio files must be as follows: 01.mp3, 02.mp3, ..., 99.mp3 (or) 01.wav, 02.wav, ..., 99.wav

# To work with mp3-files you will need to install ffmpeg and put it to PATH.
# Windows instruction here http://blog.gregzaal.com/how-to-install-ffmpeg-on-windows/
import argparse
import json
import logging
import sys
from multiprocessing import Semaphore, Process
from pathlib import Path

import speech_recognition as sr
from pydub import AudioSegment, effects
from pydub.silence import split_on_silence

from bert.bert_punctuation import BertPunctuation  # https://github.com/vlomme/Bert-Russian-punctuation don't forget to download pretrained bert model https://drive.google.com/file/d/190dLqhRjqgNJLKBqz0OxQ3TzxSm5Qbfx/view
from normalizer.normalizer import Normalizer  # https://github.com/snakers4/russian_stt_text_normalization

# Settings
SOURCE_FORMAT = 'mp3'  # or 'wav' format of source audio file.
SYMBOLS_GATE = False  # only chunks with normal symbol rate (symbols per second) will be used
SYMBOL_RATE_MIN = 13  # min amount of symbols per second audio
SYMBOL_RATE_MAX = 30  # max amount of symbols per second audio
ADDITIONAL_CLEAN = True  # before use chunk will be send to google cloud, if google can not recognize words in this chunk, it will be not used. True will consume additional time.
MIN_SILENCE_LEN = 300  # silence duration for cut in ms. If the speaker stays silent for longer, increase this value. else, decrease it.
SILENCE_THRESH = -36  # consider it silent if quieter than -36 dBFS. Adjust this per requirement.
KEEP_SILENCE = 100  # keep some ms of leading/trailing silence.
FRAME_RATE = 16000  # set the framerate of result audio.
TARGET_LENGTH = 1000  # min target length of output audio files in ms.
PUNCTUATION = False  # will add commas in text. Set it to False if you use other language as russian.

PROCESSES_NUM = 5  # Parallel processes

LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

PROGRESS_FILE = Path('progress.json')


def config_logger(name: str, filename: str) -> logging.Logger:
    """Configure logger"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = []

    formatter = logging.Formatter(f'{name}: %(message)s')

    fh = logging.FileHandler(LOG_DIR / f'{filename}.log', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.addHandler(fh)
    return logger


def silence_based_conversion(input_audio: Path, output_dir: Path, start_index: int) -> int:
    """function that splits the audio file into chunks and applies speech recognition"""
    author = input_audio.parent.parts[-1]
    rel_input = input_audio.relative_to(input_audio.parents[1])

    logger = config_logger(str(rel_input), str(author))

    try:
        progress = json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
        if progress.get(str(rel_input)):
            logger.info('Already processed, skipping')
            return progress.get(str(rel_input))
    except FileNotFoundError:
        pass

    # open the audio file stored in the local system
    if SOURCE_FORMAT == 'wav':
        logger.info('Opening')
        song = AudioSegment.from_wav(input_audio)
    else:
        logger.info('Converting to WAV')
        song = AudioSegment.from_file(input_audio, 'mp3')

    song = song.set_channels(1)

    # set the framerate of result autio
    song = song.set_frame_rate(FRAME_RATE)

    # split track where silence is 0.5 seconds or more and get chunks
    logger.info('Splitting to chunks')
    chunks = split_on_silence(song, MIN_SILENCE_LEN, SILENCE_THRESH, KEEP_SILENCE)

    # create a directory to store output files
    splitted = output_dir / author

    if splitted.exists():
        logger.info('Conversion was aborted. Continue...')

    splitted.mkdir(exist_ok=True, parents=True)

    chunk_file = splitted / 'check_temp.wav'
    temp_file = splitted / 'temp.wav'
    metadata_file = splitted / 'metadata.csv'

    # additional clean. Use it if you want to remove chunks without speech.
    if ADDITIONAL_CLEAN:
        checked_chunks = [chunks[0]]
        # check each chunk
        for chunk in chunks:
            # Create 1000 milliseconds silence chunk
            # Silent chunks (1000ms) are needed for correct working google recognition
            chunk_silent = AudioSegment.silent(duration=1000)

            # Add silent chunk to beginning and end of audio chunk.
            # This is done so that it doesn't seem abruptly sliced.
            # We will send this chunk to google recognition service
            audio_chunk_temp = chunk_silent + chunk + chunk_silent

            # specify the bitrate to be 192k
            # save chunk for google recognition as temp.wav
            audio_chunk_temp.export(chunk_file, bitrate='192k', format='wav')

            # create a speech recognition object
            r = sr.Recognizer()

            # recognize the chunk
            with sr.AudioFile(str(chunk_file)) as source:
                # remove this if it is not working correctly.
                r.adjust_for_ambient_noise(source)
                audio_listened = r.listen(source)

                try:
                    # try converting it to text
                    # if you use other language as russian, correct the language as described here https://cloud.google.com/speech-to-text/docs/languages
                    r.recognize_google(audio_listened, language='ru-RU')
                    checked_chunks.append(chunk)
                    logger.info('checking chunk - passed')
                except sr.UnknownValueError:
                    logger.info('checking chunk - not passed')

                except sr.RequestError:
                    logger.info('--- Could not request results. check your internet connection')

            # finaly remove the temp-file
            chunk_file.unlink()

        chunks = checked_chunks

    # now recombine the chunks so that the parts are at least "target_length" long
    output_chunks = [chunks[0]]
    for chunk in chunks[1:]:
        if len(output_chunks[-1]) < TARGET_LENGTH:
            output_chunks[-1] += chunk
        else:
            output_chunks.append(chunk)

    chunks = output_chunks

    logger.info(f'Found {len(chunks)} chunks')

    # Load pretrained models
    norm = Normalizer()

    # process each chunk
    for counter, chunk in enumerate(chunks, start_index):
        output_file = splitted / f'{author}_{counter:04d}.wav'
        if output_file.exists():
            logger.info(f'{output_file.relative_to(splitted)} already processed, skipping.')
            continue

        # Create 1000 milliseconds silence chunk
        # Silent chunks (1000ms) are needed for correct working google recognition
        chunk_silent = AudioSegment.silent(duration=1000)

        # Add silent chunk to beginning and end of audio chunk.
        # This is done so that it doesn't seem abruptly sliced.
        # We will send this chunk to google recognition service
        audio_chunk_temp = chunk_silent + chunk + chunk_silent

        # This chunk will be stored
        audio_chunk = chunk

        # export audio chunk and save it in the current directory.
        # normalize the loudness in audio
        audio_chunk = effects.normalize(audio_chunk)

        # specify the bitrate to be 192k
        # save chunk for google recognition as temp.wav
        audio_chunk_temp.export(temp_file, bitrate='192k', format='wav')

        logger.info(f'Processing {output_file.relative_to(splitted)}')

        # create a speech recognition object
        r = sr.Recognizer()

        # recognize the chunk
        with sr.AudioFile(str(temp_file)) as source:
            # remove this if it is not working correctly.
            r.adjust_for_ambient_noise(source)
            audio_listened = r.listen(source)

        try:
            # try converting it to text
            # if you use other language as russian, correct the language as described here https://cloud.google.com/speech-to-text/docs/languages
            rec = r.recognize_google(audio_listened, language='ru-RU').lower()

            # google recognition return numbers as integers i.e. "1, 200, 35".
            # text normalization will read this numbers and return this as a writen russian text i.e. "один, двести, тридцать пять"
            # if you use other language as russian, repalce this line
            rec = norm.norm_text(rec)

            # bert punctuation - will place commas in text
            if PUNCTUATION:
                rec = [rec]
                rec = BertPunctuation().predict(rec)
                rec = (rec[0])

            audio_length_ms = len(audio_chunk)  # in milliseconds
            audio_length_sec = float(len(audio_chunk)) / 1000  # in seconds
            symbol_count = float(len(rec))

            # here starts the filtering on symbol rate
            if SYMBOLS_GATE:
                if (symbol_count / audio_length_sec > SYMBOL_RATE_MIN) and (symbol_count / audio_length_sec < SYMBOL_RATE_MAX):
                    rate = int(symbol_count / audio_length_sec)
                    logger.info(f'Symbol rate {rate}')
                    # write the output to the metadata.csv.
                    with metadata_file.open(mode='a+', encoding='utf=8') as f:
                        f.write(f'{output_file.name}|{rec}|{rec}|{rate}|{audio_length_ms}\n')

                    # save audio file & update progress
                    audio_chunk.export(output_file, bitrate='192k', format='wav')

                else:
                    logger.info('- text too short or too long')
            else:
                # write the output to the metadata.csv.
                with metadata_file.open(mode='a+', encoding='utf=8') as f:
                    f.write(f'{output_file.name}|{rec}|{rec}\n')

                # save audio file & update progress
                audio_chunk.export(output_file, bitrate='192k', format='wav')

        # catch any errors. Audio files with errors will be not mentioned in metadata.csv
        except sr.UnknownValueError:
            logger.info('-- Could not understand audio')

        except sr.RequestError:
            logger.info('--- Could not request results. Check your internet connection')

        # finaly remove the temp-file
        temp_file.unlink()

    try:
        progress = json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
    except FileNotFoundError:
        progress = {}

    progress[str(rel_input)] = len(chunks)
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False, indent=4), encoding='utf-8')

    return progress[str(rel_input)]


def process_dir(directory: Path, output_dir: Path, semaphore: Semaphore):
    """Process all audio files in directory"""
    with semaphore:
        last_index = 0
        for audio_file in directory.glob(f'*.{SOURCE_FORMAT}'):
            last_index = silence_based_conversion(audio_file, output_dir, last_index + 1)


def main():
    """Main function"""
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir', help='Input dir')
    parser.add_argument('output_dir', help='Output dir')
    args = parser.parse_args()

    # get dirs to process
    output_dir = Path(args.output_dir)

    semaphore = Semaphore(PROCESSES_NUM)
    all_processes = []

    for author in Path(args.input_dir).glob('*'):
        if author.is_dir():
            p = Process(target=process_dir, args=(author, output_dir, semaphore))
            all_processes.append(p)
            p.start()

    for p in all_processes:
        p.join()


if __name__ == '__main__':
    main()
