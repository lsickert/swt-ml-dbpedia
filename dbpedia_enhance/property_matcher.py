import multiprocessing as mp
from data.utils import DATA_FOLDER
import csv
from typing import Any, Optional
from tqdm.auto import tqdm
from .translate_entity_new import translate_entity
import re

SPECIAL_PROPERTIES = ["url", "x", "y", "image"]


def find_matches(src_props: set, trg_props: set, src_lang: str, trg_lang: str, suffix: Optional[str] = None) -> list:
    """finds all matching properties between two languages"""
    matches = []
    src_props = clean_prop_list(src_props)
    trg_props = clean_prop_list(trg_props)
    print("### finding direct matches")
    direct_matches = find_direct_matches(src_props, trg_props)
    print(f"### {len(direct_matches)} found")

    # remove all direct matches from target set to make it smaller
    for match in direct_matches:
        matches.append((match, match))
        trg_props.discard(match)

    print("### finding entity matches")
    entity_matches = find_entity_matches(
        list(src_props), list(trg_props), src_lang, trg_lang, suffix)
    print(f"### {len(entity_matches)} enitity matches found")

    for match in entity_matches:
        matches.append(match)
        src_props.discard(match[0])
        trg_props.discard(match[1])

    out_name = f"{src_lang}_{trg_lang}"

    if suffix is not None:
        out_name = out_name + "_" + suffix

    out_file = DATA_FOLDER / f"{out_name}_matches.csv"

    with open(out_file, "w", encoding="utf-8", newline="") as out:
        out_writer = csv.writer(out)
        out_writer.writerow(["source", "target"])

        for match in matches:
            out_writer.writerow([match[0], match[1]])

        for prop in src_props:
            out_writer.writerow([prop, ""])

        for prop in trg_props:
            out_writer.writerow(["", prop])

    return matches


def find_direct_matches(src_props: set, trg_props: set) -> set:
    """ find all properties in two sets where the property names are equal"""
    return set.intersection(src_props, trg_props)


def find_entity_matches(src_props: list, trg_props: list, src_lang: str, trg_lang: str, suffix: Optional[str] = None) -> set:
    """finds all occurences where an entity of the source language matches an entity in the target language"""

    num_splits = mp.cpu_count()

    src_splits = _split_list_equal(src_props, num_splits)
    trg_splits = _split_list_equal(trg_props, num_splits)

    all_matches = []

    for trg_split in trg_splits:
        split_args = []
        trg_dict = _get_split_dict(trg_split, trg_lang, src_lang, suffix)
        for idx, src_split in enumerate(src_splits):
            split_args.append((src_split, trg_dict, src_lang, idx+1, suffix))

        tqdm.set_lock(mp.RLock())
        with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)) as pool:
            all_match_list = pool.starmap(_find_entity_matches, split_args)

        for match_list in all_match_list:
            all_matches.extend(match_list)

    return all_matches


def find_single_entity_match(src_props: list, trg_ent: str, src_lang: str, trg_lang: str, suffix: Optional[str] = None) -> set:
    """finds all occurences where an entity of the source language matches an entity in the target language for a single entity"""

    num_splits = mp.cpu_count()

    src_splits = _split_list_equal(src_props, num_splits)

    all_matches = []

    split_args = []
    trg_dict = _get_split_dict([trg_ent], trg_lang, src_lang, suffix)
    for idx, src_split in enumerate(src_splits):
        split_args.append((src_split, trg_dict, src_lang, idx+1, suffix))

    tqdm.set_lock(mp.RLock())
    with mp.Pool(processes=mp.cpu_count(), initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)) as pool:
        all_match_list = pool.starmap(_find_entity_matches, split_args)

    for match_list in all_match_list:
        all_matches.extend(match_list)

    return all_matches


def _get_split_dict(prop_list: list, trg_lang: str, src_lang: str, suffix: Optional[str] = None) -> dict:

    path_name = trg_lang

    if suffix is not None:
        path_name = path_name + "_" + suffix

    prop_path = DATA_FOLDER / path_name

    size = len(prop_list)

    prop_dict = {}

    with tqdm(total=size) as pbar:
        for prop in prop_list:
            trg_entities = []

            pbar.update(1)
            try:
                with open(prop_path / f"{prop}.csv", "r", newline="", encoding="utf-8") as csv_trg_file:
                    csv_trg_reader = csv.reader(csv_trg_file)

                    # skip first row with headers
                    next(csv_trg_reader, None)
                    val_list = []
                    subj_list = []
                    form_list = []
                    for row in csv_trg_reader:
                        subj_list.append(row[0])
                        val_list.append(row[1])
                        form_list.append(row[2])

                        if len(subj_list) == 40:

                            subj_trans = translate_entity(
                                subj_list, trg_lang, [src_lang])

                            for idx, subj in enumerate(subj_trans):
                                trans = subj.get(src_lang)
                                if trans is not None:
                                    subj_list[idx] = trans

                            val_trans_src = []
                            val_trans_ids = []
                            for idx, val in enumerate(val_list):
                                if form_list[idx] == "instance":
                                    val_trans_src.append(val)
                                    val_trans_ids.append(idx)

                            if len(val_trans_src) > 0:
                                val_trans = translate_entity(
                                    val_trans_src, trg_lang, [src_lang])

                                for idx, idy in enumerate(val_trans_ids):
                                    val_list[idy] = val_trans[idx].get(
                                        src_lang, val_list[idy])

                            for idx, subj in enumerate(subj_list):
                                trg_entities.append(
                                    [subj, val_list[idx], form_list[idx]])

                            val_list = []
                            subj_list = []
                            form_list = []

                    if len(subj_list) > 0:

                        subj_trans = translate_entity(
                            subj_list, trg_lang, [src_lang])

                        for idx, subj in enumerate(subj_trans):
                            trans = subj.get(src_lang)
                            if trans is not None:
                                subj_list[idx] = trans

                        val_trans_src = []
                        val_trans_ids = []
                        for idx, val in enumerate(val_list):
                            if form_list[idx] == "instance":
                                val_trans_src.append(val)
                                val_trans_ids.append(idx)

                        if len(val_trans_src) > 0:
                            val_trans = translate_entity(
                                val_trans_src, trg_lang, [src_lang])

                            for idx, idy in enumerate(val_trans_ids):
                                val_list[idy] = val_trans[idx].get(
                                    src_lang, val_list[idy])

                        for idx, subj in enumerate(subj_list):
                            trg_entities.append(
                                [subj, val_list[idx], form_list[idx]])

                prop_dict[prop] = trg_entities

            except Exception as e:
                print(str(e))
                continue

    return prop_dict


def _find_entity_matches(src_props: list, trg_lang_props: dict, src_lang: str, pid: int, suffix: Optional[str] = None) -> list:
    """find an entity with a given property in one language that also exists in another language"""

    matched_props = []
    size = len(src_props)
    desc = f"#{pid}"

    def compare_entities(src_ents, trg_ents):
        # TODO: figure out how to handle multiple matching properties
        max_matches = 0.6 * min(len(src_ents), len(trg_ents))
        matches = 0

        for trg_ent in trg_ents:
            for src_ent in src_ents:
                if src_ent[0] == trg_ent[0] and src_ent[1] == trg_ent[1]:
                    matches += 1

                    if matches >= max_matches:
                        return True

        return False

    src_path_name = src_lang

    if suffix is not None:
        src_path_name = src_path_name + "_" + suffix

    with tqdm(total=size, desc=desc, position=pid) as pbar:
        for src_property in src_props:
            src_path = DATA_FOLDER / src_path_name / f"{src_property}.csv"

            src_entities = []
            pbar.update(1)

            try:
                with open(src_path, "r", newline="", encoding="utf-8") as csv_src_file:
                    csv_src_reader = csv.reader(csv_src_file)

                    # skip first row with headers
                    next(csv_src_reader, None)
                    for row in csv_src_reader:
                        src_entities.append(row)

                for prop, entities in trg_lang_props.items():

                    if compare_entities(src_entities, entities):
                        matched_props.append((src_property, prop))
                        break

            except Exception as e:
                print(str(e))
                continue

    return matched_props


def clean_prop_list(props: set) -> set:
    """remove properties from the property list that are very likely parsing errors"""
    cleaned_props = set()

    for prop in props:
        # remove encapsulated properties
        if prop.startswith("\""):
            continue
        # anything with a % inside is very likely wrongly parsed formatting
        if prop.find("%") > -1:
            continue
        # remove everything without letters or digits
        if re.search(r"[a-zA-Z\d]", prop) is None:
            continue
        # remove certain special properties, where we already know that they match and/or matches might not hold any significant value
        if prop in SPECIAL_PROPERTIES:
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
