import multiprocessing as mp
from data.utils import DATA_FOLDER
import csv
from typing import Any


def find_matches(src_props: set, trg_props: set) -> list:

    matches = []
    direct_matches = find_direct_match(src_props, trg_props)

    # remove all direct matches from both sets to make them smaller
    for match in direct_matches:
        matches.append((match, match))
        src_props.discard(match)
        trg_props.discard(match)


def find_direct_match(src_props: set, trg_props: set) -> set:
    """ find all properties in two sets where the property names are equal"""
    return set.intersection(src_props, trg_props)


def _find_entity_match(property: str, trg_lang_props: set, src_lang: str, trg_lang: str):
    """ find an entity with a given property in one language that also exists in another language"""

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

                #TODO: add translation of target entitiy to source language here

        matches = []
        for ent in trg_entities:
            match = _is_in_2d_list(property_entities, ent[0], 0) and _is_in_2d_list(
                property_entities, ent[1], 1)
            matches.append(match)

        if len(matches) > 0.8/len(property_entities):
            return True


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
