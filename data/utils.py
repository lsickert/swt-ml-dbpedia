import io
import shutil
import requests
import bz2
from tqdm.auto import tqdm
from pathlib import Path
from functools import partial

DATA_FOLDER = Path(__file__).parent.resolve()


def get_data(force_redownload=False):

    if not Path(DATA_FOLDER / "infobox-properties_lang=de.ttl.bz2").exists() or force_redownload:
        fname = download_file(
            "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=de.ttl.bz2")
        extract_file(fname)

    if not Path(DATA_FOLDER / "infobox-properties_lang=en.ttl.bz2").exists() or force_redownload:
        fname = download_file(
            "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=en.ttl.bz2")
        extract_file(fname)

    if not Path(DATA_FOLDER / "infobox-properties_lang=nl.ttl.bz2").exists() or force_redownload:
        fname = download_file(
            "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=nl.ttl.bz2")
        extract_file(fname)
    return


def download_file(url):
    filename = url.split('/')[-1]

    with requests.get(url, stream=True) as res:
        if res.status_code != 200:
            res.raise_for_status()
            raise RuntimeError(f"{url} returned {res.status_code} status")

        size = int(res.headers.get('Content-Length', 0))

        res.raw.read = partial(res.raw.read, decode_content=True)

        desc = f"downloading {filename}"
        with tqdm.wrapattr(res.raw, "read", total=size, desc=desc) as raw_res:
            with open(DATA_FOLDER / filename, 'wb') as file:
                shutil.copyfileobj(raw_res, file)

        return filename


def extract_file(file):
    fname = file[:-4]
    desc = f"extracting {file}"
    compfile = bz2.open(DATA_FOLDER / file)
    size = _get_file_size(compfile)
    with tqdm.wrapattr(compfile, "read", total=size, desc=desc) as comp:
        with open(DATA_FOLDER / fname, "wb") as out:
            shutil.copyfileobj(comp, out)


def _get_file_size(f):
    cur = f.tell()
    f.seek(0, io.SEEK_END)
    size = f.tell()
    f.seek(cur)
    return size
