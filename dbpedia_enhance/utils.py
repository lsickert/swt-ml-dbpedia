from typing import Tuple


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


def create_rdf_value(val: str, type: str, lang: str = None) -> str:
    """Creates an rdf-conform dbpedia value form the raw data"""
    if type == "instance":
        return f"<http://{lang}.dbpedia.org/resource/{val}>"
    elif type == "string":
        return f"\"{val}\"@{lang}"
    elif type == "other":
        return val
    else:
        return f"\"{val}\"^^<http://www.w3.org/2001/XMLSchema#{type}>"
