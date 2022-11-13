from typing import Tuple, Optional
import requests
import time
import random


def get_lang_code(fname: str) -> str:
    """extracts the language code from a dbpedia file name"""
    lang_idx = fname.find("lang=")
    lang_code = fname[lang_idx+5:lang_idx+7]

    return lang_code


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


def create_rdf_subj(subject: str, lang: str) -> str:
    """creates and rdf conforming dbpedia subject from the raw data"""
    return f"<http://{lang}.dbpedia.org/resource/{subject}>"


def create_rdf_prop(prop: str, lang: str) -> str:
    """creates an rdf-conform dbpedia property from the raw data"""
    return f"<http://{lang}.dbpedia.org/property/{prop}>"


def create_rdf_value(val: str, typ: str, lang: str = None) -> str:
    """Creates an rdf-conform dbpedia value form the raw data"""
    if type == "instance":
        return f"<http://{lang}.dbpedia.org/resource/{val}>"
    elif type == "string":
        return f"\"{val}\"@{lang}"
    elif type == "other":
        return val
    else:
        return f"\"{val}\"^^<http://www.w3.org/2001/XMLSchema#{typ}>"


def get_category_members(category: str, lang: str, continue_val: Optional[str] = None, part_results: Optional[list] = None) -> list:
    """returns a list of entity names that are members of a given category."""

    base_url = f"https://{lang}.wikipedia.org/w/api.php"

    results = set()

    params = {
        "action": "query",
        "cmtitle": category,
        "list": "categorymembers",
        "cmlimit": "max",
        "cmtype": "page",
        "formatversion": "2",
        "format": "json"
    }

    # handle recursion
    if continue_val is not None:
        params["cmcontinue"] = continue_val

    if part_results is not None:
        results = part_results

    cont_req = True

    while cont_req:
        with requests.get(base_url, params=params, timeout=5) as res:
            if res.status_code != 200:
                # catch rate limiting errors and try to distribute load a bit better
                if res.status_code == 429:
                    time.sleep(random.randint(1, 10))
                    return get_category_members(category, lang, continue_val, results)
                res.raise_for_status()
                raise RuntimeError(
                    f"{base_url} returned {res.status_code} status")

            data = res.json()

            if not "continue" in data.keys():
                cont_req = False

            for item in data["query"]["categorymembers"]:
                results.add(item["title"].replace(" ", "_"))
            
            if cont_req:
                continue_val = data["continue"]["cmcontinue"]
                params["cmcontinue"] = continue_val

    return list(results)
