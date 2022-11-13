import multiprocessing as mp
import os
import csv
from tqdm import tqdm
from pathlib import Path
from data.utils import DATA_FOLDER
from .utils import extract_subj_name, get_lang_code, get_category_members, extract_value
from typing import Optional


def extract_types(file: str, suffix: Optional[str] = None, use_category: Optional[str] = None, force: Optional[bool] = False):
    """
    extract all value types from a language file.
    """

    lang_code = get_lang_code(file)

    filtr = None

    if use_category is not None:
        filtr = get_category_members(use_category, lang_code)

    if suffix is not None:
        lang_code = lang_code + "_" + suffix

    type_file = DATA_FOLDER / f"{lang_code}_types.csv"

    all_types = set()

    if type_file.exists() and not force:
        with open(type_file, "r", newline="", encoding="utf-8") as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                all_types.update(row)

        return all_types

    chunk_args = _get_chunks(DATA_FOLDER / file)

    pool_args = []
    for idx, arg in enumerate(chunk_args):
        new_arg = (*arg, filtr, idx+1)
        pool_args.append(new_arg)

    with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(mp.RLock(),)) as pool:
        all_type_list = pool.starmap(_extract_types, pool_args)


    for types in all_type_list:
        all_types.update(types)

    with open(type_file, "w", encoding="utf-8", newline="") as out:
        out_writer = csv.writer(out)
        for sub in all_types:
            out_writer.writerow([sub])

    return all_types

def _extract_types(file: Path, chunk_start: int, chunk_end: int, size: int, filtr: Optional[list], pid: int) -> set:
    """
    extracts the types from an rdf file. Returns a set with all individual type names.
    When a filter is present we still try to extract subjects through the file to make sure that they are present in the exported data.
    """
    all_types = set()

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
                value, form = extract_value(content[2])

                if filtr is None or subject in filtr:

                    all_types.add(form)

                pbar.update(len(line.encode("utf-8")))

    return all_types


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
