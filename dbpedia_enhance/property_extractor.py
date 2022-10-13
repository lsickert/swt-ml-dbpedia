import multiprocessing as mp
import os
import csv
from tqdm import tqdm
from filelock import FileLock
from pathlib import Path
from data.utils import DATA_FOLDER
from typing import Tuple


def extract_properties(file: str):
    """
    extract all properties from a language file and stores the results in individual lists. 
    Additionally a single csv file containing all distinct property names is created
    """
    lang_idx = file.find("lang=")
    lang_code = file[lang_idx+5:lang_idx+7]

    out_path = DATA_FOLDER / lang_code

    _check_dir_exists(out_path)

    chunk_args = _get_chunks(DATA_FOLDER / file, out_path)

    pool_args = []
    for idx, arg in enumerate(chunk_args):
        new_arg = (*arg, idx+1)
        pool_args.append(new_arg)

    with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(mp.RLock(),)) as pool:
        all_prop_list = pool.starmap(_extract_properties, pool_args)

    for file in out_path.iterdir():
        if file.is_file() and file.suffix == ".lock":
            file.unlink(missing_ok=True)

    all_properties = set()
    for properties in all_prop_list:
        all_properties.update(properties)

    with open(DATA_FOLDER / f"{lang_code}_properties.csv", "a", encoding="utf-8", newline="") as out:
        out_writer = csv.writer(out)
        for prop in all_properties:
            out_writer.writerow([prop])


def extract_subj_name(subject: str) -> str:
    """extracts the name of a subject from rdf syntax"""
    return subject.split("resource/")[-1]


def extract_prop_name(prop: str) -> str:
    """extracts the name of a property from rdf syntax"""
    return prop.split("property/")[-1]


def extract_value(value: str) -> Tuple[str, str]:
    """extracts the value of an rdf triple"""
    # remove turtle syntax at the end
    value = value[:-2]
    value = value.strip()

    # detect if value is text or not
    if value.endswith(">"):
        value = value[:-1]
        if value.find("resource/") != -1:
            return value.split("resource/")[-1], "instance"
        else:
            val_list = value.split("^^")
            if len(val_list) > 1:
                typ = val_list[-1].split("#")[-1]
                value = val_list[0]
                if value.endswith('"'):
                    return value[1:-1], typ
                return value, typ
            else:
                value = val_list[0] + ">"
                return value, "other"
    else:
        value = value.split("@")[0]
        if value.startswith('"'):
            value = value[1:-1]
        return value, "string"


def _extract_properties(file: Path, chunk_start: int, chunk_end: int, size: int, out_folder: Path, pid: int) -> set:
    """extracts the properties from an rdf file and saves them into individual files. Returns a set with all individual property names"""
    all_props = set()

    desc = f"#{pid}"

    with tqdm(total=size, desc=desc, position=pid) as pbar:
        with open(file, "r", encoding="utf-8") as f:
            f.seek(chunk_start)

            for line in f:
                chunk_start += len(line)
                if chunk_start > chunk_end:
                    break

                try:
                    content = line.split("> ", 2)
                    subject = extract_subj_name(content[0])
                    prop = extract_prop_name(content[1])
                    value, form = extract_value(content[2])

                    all_props.add(prop)

                    out_file = out_folder / f"{prop}.csv"
                    lock_file = out_folder / f"{prop}.csv.lock"
                    lock = FileLock(str(lock_file))

                    with lock:
                        if not out_file.exists():
                            with open(out_file, "a", encoding="utf-8", newline="") as out:
                                out_writer = csv.writer(out)
                                out_writer.writerow(
                                    ["subject", "value", "format"])
                                out_writer.writerow([subject, value, form])
                        else:
                            with open(out_file, "a", encoding="utf-8", newline="") as out:
                                out_writer = csv.writer(out)
                                out_writer.writerow([subject, value, form])
                except BaseException as e:
                    err_file = out_folder / "_err.log"
                    lock_file = out_folder / "_err.log.lock"
                    lock = FileLock(str(lock_file))

                    with lock:
                        with open(err_file, "a", encoding="utf-8") as err_f:
                            err_f.write(line + " || Error: " + str(e) + "\n")
                pbar.update(len(line.encode("utf-8")))

    return all_props


def _get_chunks(file: Path, out: Path) -> list:
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

            args = (file, chunk_start, chunk_end, size, out)
            chunk_args.append(args)

            chunk_start = chunk_end

    return chunk_args


def _check_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)