import multiprocessing as mp
from data.utils import DATA_FOLDER
import csv
from typing import Any
from tqdm import tqdm


def find_matches(src_props: set, trg_props: set, src_lang: str, trg_lang: str) -> list:
    """finds all matching propeerties between two languages"""
    matches = []
    direct_matches = find_direct_matches(src_props, trg_props)

    # remove all direct matches from both sets to make them smaller
    for match in direct_matches:
        matches.append((match, match))
        src_props.discard(match)
        trg_props.discard(match)

    entity_matches = find_entity_matches(
        src_props, trg_props, src_lang, trg_lang)

    for match in entity_matches:
        matches.append(match)
        src_props.discard(match[0])
        trg_props.discard(match[1])


def find_direct_matches(src_props: set, trg_props: set) -> set:
    """ find all properties in two sets where the property names are equal"""
    return set.intersection(src_props, trg_props)


def find_entity_matches(src_props: set, trg_props: set, src_lang: str, trg_lang: str) -> set:
    """finds all occurences where an entity of the source language matches an entity in the target language"""

    num_splits = mp.cpu_count()

    splits = _split_list_equal(src_props, num_splits)

    split_args = []
    for s in range(num_splits):
        split_args.append(splits[s], trg_props, src_lang, trg_lang, s)

    with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(mp.RLock(),)) as pool:
        all_match_list = pool.starmap(_find_entity_matches, split_args)

    all_matches = []

    for match_list in all_match_list:
        all_matches.extend(match_list)

    return match_list


def _find_entity_matches(src_props: set, trg_lang_props: set, src_lang: str, trg_lang: str, pid: int) -> list:
    """find an entity with a given property in one language that also exists in another language"""

    matched_props = []
    size = len(src_props)
    desc = f"#{pid}"

    with tqdm(total=size, desc=desc, position=pid) as pbar:
        for property in src_props:
            src_path = DATA_FOLDER / src_lang / property
            trg_lang_path = DATA_FOLDER / trg_lang

            property_entities = []
            with open(src_path, "r", newline="", encoding="utf-8") as csvfile:
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                    property_entities.append(row)

            for prop in trg_lang_props:
                trg_entities = []

                with open(trg_lang_path / prop, "r", newline="", encoding="utf-8") as csvfile:
                    csvreader = csv.reader(csvfile)

                    for row in csvreader:
                        trg_entities.append(row)

                        # TODO: add translation of target entitiy to source language here

                matches = []
                for ent in trg_entities:
                    match = _is_in_2d_list(property_entities, ent[0], 0) and _is_in_2d_list(
                        property_entities, ent[1], 1)
                    matches.append(match)

                # TODO: figure out how to handle multiple matching properties
                if len(matches) > 0.8/len(property_entities):
                    matched_props.append((property, prop))
                    break

            pbar.update(1)

    return matched_props


def _is_in_2d_list(list: list, val: Any, trg_col: int = None) -> bool:
    """returns True if a value is found in a 2-dimensional list, otherwise returns false"""
    for row in list:
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
