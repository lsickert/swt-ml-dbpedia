import multiprocessing as mp
import os
import csv
from tqdm import tqdm
from pathlib import Path
from data.utils import DATA_FOLDER
from .utils import extract_subj_name, get_lang_code


def extract_subjects(file: str):
    """
    extract all subjects from a language file and stores the results in individual lists. 
    Additionally a single csv file containing all distinct subject names is created
    """

    lang_code = get_lang_code(file)

    chunk_args = _get_chunks(DATA_FOLDER / file)

    pool_args = []
    for idx, arg in enumerate(chunk_args):
        new_arg = (*arg, idx+1)
        pool_args.append(new_arg)

    with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(mp.RLock(),)) as pool:
        all_sub_list = pool.starmap(_extract_subjects, pool_args)

    all_subjects = set()
    for subjects in all_sub_list:
        all_subjects.update(subjects)

    with open(DATA_FOLDER / f"{lang_code}_subjects.csv", "w", encoding="utf-8", newline="") as out:
        out_writer = csv.writer(out)
        for sub in all_subjects:
            out_writer.writerow([sub])

    return all_subjects

def _extract_subjects(file: Path, chunk_start: int, chunk_end: int, size: int, pid: int) -> set:
    """extracts the subjects from an rdf file and saves them into individual files. Returns a set with all individual subject names"""
    all_subjects = set()

    desc = f"#{pid}"

    with tqdm(total=size, desc=desc, position=pid) as pbar:
        with open(file, "r", encoding="utf-8") as f:
            f.seek(chunk_start)

            for line in f:
                chunk_start += len(line)
                if chunk_start > chunk_end:
                    break

                content = line.split("> ", 2)
                subject = extract_subj_name(content[0])

                all_subjects.add(subject)

                pbar.update(len(line.encode("utf-8")))

    return all_subjects


def _get_chunks(file: Path) -> list:
    """split a file into smaller chunks for multiprocessing"""
    # multiprocessing code adapted from https://nurdabolatov.com/parallel-processing-large-file-in-python

    cpus = mp.cpu_count()
    fsize = os.path.getsize(file)
    chunk_size = fsize // cpus

    chunk_args = []

    with open(file, "r", encoding="utf-8") as f:

        def is_start_of_line(pos):
            if pos == 0:
                return True

            f.seek(pos - 1)
            try:
                return f.read(1) == "\n"
            except UnicodeDecodeError:
                return False

        def get_next_line_position(pos):
            f.seek(pos)
            f.readline()
            return f.tell()

        chunk_start = 0

        while chunk_start < fsize:
            chunk_end = min(fsize, chunk_start + chunk_size)

            while not is_start_of_line(chunk_end):
                chunk_end -= 1

            if chunk_start == chunk_end:
                chunk_end = get_next_line_position(chunk_end)

            size = chunk_end - chunk_start

            args = (file, chunk_start, chunk_end, size)
            chunk_args.append(args)

            chunk_start = chunk_end

    return chunk_args


def _check_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
