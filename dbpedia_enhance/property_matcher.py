import multiprocessing as mp
from data.utils import DATA_FOLDER
import csv
from typing import Any
from tqdm.auto import tqdm
import re


def find_matches(src_props: set, trg_props: set, src_lang: str, trg_lang: str) -> list:
    """finds all matching propeerties between two languages"""
    matches = []
    src_props = clean_prop_list(src_props)
    trg_props = clean_prop_list(trg_props)
    print("### finding direct matches")
    direct_matches = find_direct_matches(src_props, trg_props)
    print(f"### {len(direct_matches)} found")

    # remove all direct matches from target set to make them smaller
    for match in direct_matches:
        matches.append((match, match))
        src_props.discard(match)

    print("### finding entity matches")
    entity_matches = find_entity_matches(
        src_props, trg_props, src_lang, trg_lang)
    print(f"### {len(entity_matches)} enitity matches found")

    for match in entity_matches:
        matches.append(match)
        src_props.discard(match[0])
        trg_props.discard(match[1])

    out_file = DATA_FOLDER / f"{src_lang}-{trg_lang}_matches.csv"

    with open(out_file, "w", encoding="utf-8", newline="") as out:
        out_writer = csv.writer(out)
        out_writer.writerow(["source", "target"])

        for match in matches:
            out_writer.writerow([match[0], match[1]])

        for prop in src_props:
            out_writer.writerow([prop, ""])

        for prop in trg_props:
            out_writer.writerow(["", prop])


def find_direct_matches(src_props: set, trg_props: set) -> set:
    """ find all properties in two sets where the property names are equal"""
    return set.intersection(src_props, trg_props)


def find_entity_matches(src_props: list, trg_props: list, src_lang: str, trg_lang: str) -> set:
    """finds all occurences where an entity of the source language matches an entity in the target language"""

    num_splits = mp.cpu_count()

    src_splits = _split_list_equal(src_props, num_splits)
    trg_splits = _split_list_equal(trg_props, num_splits)

    all_matches = []

    for s in range(2):
        split_args = []
        trg_dict = _get_split_dict(trg_splits[s], trg_lang)
        for idx, src_split in enumerate(src_splits):
            split_args.append((src_split, trg_dict, src_lang, trg_lang, idx+1))

        tqdm.set_lock(mp.RLock())
        with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)) as pool:
            all_match_list = pool.starmap(_find_entity_matches, split_args)

        for match_list in all_match_list:
            all_matches.extend(match_list)

    return all_match_list


def _get_split_dict(prop_list: list, lang: str) -> dict:

    prop_path = DATA_FOLDER / lang

    size = len(prop_list)

    prop_dict = {}

    with tqdm(total=size) as pbar:
        for prop in prop_list:
            trg_entities = []

            pbar.update(1)
            try:
                with open(prop_path / f"{prop}.csv", "r", newline="", encoding="utf-8") as csv_trg_file:
                    csv_trg_reader = csv.reader(csv_trg_file)

                    for row in csv_trg_reader:
                        if not row[0] == "subject":
                            trg_entities.append(row)

                prop_dict[prop] = trg_entities
                # TODO: add translation of target entitiy to source language here
            except:
                continue

    return prop_dict


def _find_entity_matches(src_props: list, trg_lang_props: dict, src_lang: str, trg_lang: str, pid: int) -> list:
    """find an entity with a given property in one language that also exists in another language"""

    matched_props = []
    size = len(src_props)
    desc = f"#{pid}"

    with tqdm(total=size, desc=desc, position=pid) as pbar:
        for src_property in src_props:
            src_path = DATA_FOLDER / src_lang / f"{src_property}.csv"

            src_entities = []
            src_ent_len = len(src_entities)
            pbar.update(1)

            try:
                with open(src_path, "r", newline="", encoding="utf-8") as csv_src_file:
                    csv_src_reader = csv.reader(csv_src_file)
                    for row in csv_src_reader:
                        if not row[0] == "subject":
                            src_entities.append(row)

                for prop, entities in trg_lang_props.items():
                    matches = 0
                    for ent in entities:
                        for src_ent in src_entities:
                            if src_ent[0] == ent[0] and src_ent[1] == ent[1]:
                                matches += 1

                    # TODO: figure out how to handle multiple matching properties
                    if matches >= 0.8 * min(src_ent_len,len(entities)):
                        matched_props.append((src_property, prop))
                        break

            except:
                continue

    return matched_props

def clean_prop_list(props: set) -> set:
    """remove properties from the property list, that are very likely parsing errors"""
    cleaned_props = set()

    for prop in props:
        #remove encapsulated properties
        if prop.startsWith("\""):
            continue
        # anything with a % inside is very likely wrongly parsed formatting
        if prop.find("%") > -1:
            continue
        # remove everything without letters or digits
        if re.search(r"[a-zA-Z\d]", prop) is None:
            continue
        cleaned_props.add(prop)
    
    return cleaned_props

def _is_in_2d_list(l: list, val: Any, trg_col: int = None) -> bool:
    """returns True if a value is found in a 2-dimensional list, otherwise returns false"""
    for row in l:
        try:
            col = row.index(val)
        except ValueError:
            continue
        if trg_col is None:
            return True
        else:
            return col == trg_col
    return False


def _split_list_equal(l: list, s: int) -> list:
    c = len(l) // s
    r = len(l) % s
    return [l[n * c + min(n, r):(n+1) * c + min(n+1, r)] for n in range(s)]
