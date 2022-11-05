import requests
import csv
from tqdm.auto import tqdm
import multiprocessing as mp
import time
import random
from typing import Union


def translate_entities(entities: set, src_lang: str, langcodes: list) -> list:
    """retrieve translations for a list of entities"""

    translations = []

    for entity in entities:
        translation = translate_entity(entity, src_lang, langcodes)
        translations.append(translation)

    return translations


def translate_entity(entity_list: Union[list, str], src_lang: str, langcodes: list) -> dict:
    """retrive translations for a single entity"""

    url = f"https://{src_lang}.wikipedia.org/w/api.php"

    if isinstance(entity_list, str):
        query = entity_list
    else:

        query = entity_list[0]

        if len(entity_list) > 1:
            for i in range(1, len(entity_list)):
                query += f"|{entity_list[i]}"

    params = {
        "action": "query",
        "titles": query,
        "prop": "langlinks",
        "lllimit": "500",
        "formatversion": "2",
        "format": "json"
    }

    with requests.get(url, params=params, timeout=5) as res:
        if res.status_code != 200:
            # catch rate limiting errors and try to distribute load a bit better
            if res.status_code == 429:
                time.sleep(random.randint(1, 10))
                return translate_entity(entity_list, src_lang, langcodes)
            res.raise_for_status()
            raise RuntimeError(f"{url} returned {res.status_code} status")

        data = res.json()

        results = []

        for item in data["query"]["pages"]:
            result = dict()
            result[src_lang] = item["title"].replace(" ", "_")
            if not "langlinks" in item.keys():
                continue
            for language_info in item["langlinks"]:
                if language_info["lang"] in langcodes:
                    result[language_info["lang"]
                           ] = language_info["title"].replace(" ", "_")
            results.append(result)
        return results


def run_translate():

    from . import utils, entity_extractor_new
    from data.utils import DATA_FOLDER

    trans_file = DATA_FOLDER / "subj_translations.csv"

    ALL_LANG_FILES = [
        "infobox-properties_lang=de.ttl",
        "infobox-properties_lang=en.ttl",
        "infobox-properties_lang=nl.ttl"
    ]

    all_subj = set()
    lang_codes = []
    for fname in ALL_LANG_FILES:
        lang_codes.append(utils.get_lang_code(fname))

    num_splits = mp.cpu_count()

    for fname in ALL_LANG_FILES:
        lang = utils.get_lang_code(fname)
        subj_split = _split_set_equal(
            entity_extractor_new.extract_subjects(fname), num_splits)

        trg_langs = [l for l in lang_codes if l != lang]

        split_args = []

        for idx, split in enumerate(subj_split):
            split_args.append((split, lang, trg_langs, lang_codes, idx+1))

        tqdm.set_lock(mp.RLock())
        with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)) as pool:
            trans_list = pool.starmap(_run_translate, split_args)

        for trans in trans_list:
            all_subj.update(trans)

    with tqdm(total=len(all_subj)) as pbar:
        with open(trans_file, "w", encoding="utf-8", newline="") as out:
            out_writer = csv.writer(out)
            out_writer.writerow(lang_codes)
            for sub in all_subj:
                out_writer.writerow(list(sub))
                pbar.update(1)


def _run_translate(subj: set, lang: str, trg_langs: list, lang_codes: list, pid: int):

    trans_subj = set()
    desc = f"#{pid}"
    with tqdm(total=len(subj), desc=desc, position=pid) as pbar:
        ent_list = []
        for ent in subj:
            ent_list.append(ent)
            if len(ent_list) == 40:
                trans_ents = translate_entity(
                    ent_list, lang, trg_langs)
                for trans_ent in trans_ents:
                    items = []

                    # ensure that all added tuples have the same ordering and we do not add duplicate ones
                    for l in lang_codes:
                        items.append(trans_ent.get(l, ""))

                    trans_subj.add(tuple(items))
                ent_list = []
                pbar.update(40)

        if len(ent_list) > 0:
            trans_ents = translate_entity(ent_list, lang, trg_langs)
            for trans_ent in trans_ents:
                items = []

                # ensure that all added tuples have the same ordering and we do not add duplicate ones
                for l in lang_codes:
                    items.append(trans_ent.get(l, ""))

                trans_subj.add(tuple(items))
            pbar.update(len(ent_list))

    return trans_subj


def _split_set_equal(l: set, s: int) -> list:
    l = list(l)
    c = len(l) // s
    r = len(l) % s
    return [l[n * c + min(n, r):(n+1) * c + min(n+1, r)] for n in range(s)]


if __name__ == "__main__":

    resp = translate_entity(["Mychajlo_Wosnjak", "Johanneskirche_(Kornat)", "Gerhard_R._Steinhäuser", "Ludwigstraße_8_(Bad_Kissingen)", "Mathias_Podhorsky",
                            "Jean-François_Boch", "Ulrich_Schumacher_(Kunsthistoriker)", "Willy_Großmann", "Männerwohnheim_Meldemannstraße"], "de", ["en", "nl"])
    print(resp)
