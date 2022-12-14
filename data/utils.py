import io
import shutil
import requests
import bz2
import multiprocessing as mp
from tqdm import tqdm
from pathlib import Path
from functools import partial

DATA_FOLDER = Path(__file__).parent.resolve()


def get_data(urllist: list, force_redownload: bool = False) -> list:
    """download and extract the relevant rdf files from dbpedia"""
    arglist = []

    for idx, url in enumerate(urllist):
        args = (url, force_redownload, idx)
        arglist.append(args)

    cpus = mp.cpu_count()
    pool_size = min(len(urllist), cpus)

    pool = mp.Pool(processes=pool_size, initializer=tqdm.set_lock,
                   initargs=(mp.RLock(),))

    filenames = pool.starmap(_get_file, arglist)

    return filenames


def _get_file(url: str, force_redownload: bool = False, pid=None) -> str:
    """downloads and extracts a single file from dbpedia"""
    fname = url.split("/")[-1]

    if not Path(DATA_FOLDER / fname).exists() or force_redownload:
        _download_file(url, fname, pid)
        if fname.endswith(".bz2"):
            fname = _extract_file(fname, pid)

    #remove compression encoding in case file was not redownloaded
    if fname.endswith(".bz2"):
        fname = fname[:-4]

    return fname


def _download_file(url: str, filename: str, pid) -> None:
    """downloads a file from dbpedia"""
    with requests.get(url, stream=True, timeout=5) as res:
        if res.status_code != 200:
            res.raise_for_status()
            raise RuntimeError(f"{url} returned {res.status_code} status")

        size = int(res.headers.get("Content-Length", 0))

        res.raw.read = partial(res.raw.read, decode_content=True)

        desc = f"downloading {filename}"
        with tqdm.wrapattr(res.raw, "read", total=size, desc=desc, position=pid) as raw_res:
            with open(DATA_FOLDER / filename, "wb") as file:
                shutil.copyfileobj(raw_res, file)


def _extract_file(file: str, pid) -> str:
    """extracts a file from dbpedia in bz2 format"""
    fname = file[:-4]
    desc = f"extracting {file}"
    compfile = bz2.open(DATA_FOLDER / file)
    size = _get_file_size(compfile)
    with tqdm.wrapattr(compfile, "read", total=size, desc=desc, position=pid) as comp:
        with open(DATA_FOLDER / fname, "wb") as out:
            shutil.copyfileobj(comp, out)

    return fname


def _get_file_size(f):
    """returns the size of a file object"""
    cur = f.tell()
    f.seek(0, io.SEEK_END)
    size = f.tell()
    f.seek(cur)
    return size