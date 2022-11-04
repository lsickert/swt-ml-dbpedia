import requests
import csv
from data.utils import DATA_FOLDER

def translate_entities(entities: set, src_lang: str, langcodes: list) -> list:
    """retrieve translations for a list of entities"""

    translations = []

    for entity in entities:
        translation = translate_entity(entity, src_lang, langcodes)
        translations.append(translation)

    return translations


def translate_entity(entity: str, src_lang: str, langcodes: list) -> dict:
    """retrive translations for a single entity"""

    url = f"https://{src_lang}.wikipedia.org/w/api.php"

    params = {
        "action": "query",
        "titles": entity,
        "prop": "langlinks",
        "lllimit": "500",
        "formatversion": "2",
        "format": "json"
    }

    with requests.get(url, params=params, timeout=5) as res:
        if res.status_code != 200:
            res.raise_for_status()
            raise RuntimeError(f"{url} returned {res.status_code} status")

        data = res.json()

        results = dict()
        results[src_lang] = entity

        for item in data["query"]["pages"]:
            for language_info in item["langlinks"]:
                if language_info["lang"] in langcodes:
                    results[language_info["lang"]
                            ] = language_info["title"].replace(" ", "_")

        return results


if __name__ == "__main__":
    import entity_extractor_new
    import utils

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

    for fname in ALL_LANG_FILES:
        lang = utils.get_lang_code(fname)
        subj = entity_extractor_new.extract_subjects(fname)

        trg_langs = [l for l in lang_codes if l != lang]

        for ent in subj:
            trans_ent = translate_entity(ent, lang, trg_langs)
            items = []

            # ensure that all added tuples have the same ordering and we do not add additional
            for l in lang_codes:
                items.append(subj.get(l,"") )

            all_subj.add(tuple(items))

    with open(trans_file, "w", encoding="utf-8", newline="") as out:
        out_writer = csv.writer(out)
        out_writer.writerow(lang_codes)
        for sub in all_subj:
            out_writer.writerow(list(sub))

    #result = translate_entity("University_of_Groningen", "en", ["de", "nl"])
    #print(result)
