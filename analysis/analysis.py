from tqdm.auto import tqdm
from data.utils import DATA_FOLDER
import csv
import matplotlib.pyplot as plt


def get_prop_distribution(properties: set, lang: str) -> list:

    path_name = DATA_FOLDER / lang

    size = len(properties)

    prop_lengths = []

    with tqdm(total=size) as pbar:
        for prop in properties:
            pbar.update(1)
            try:
                with open(path_name / f"{prop}.csv", "r", newline="", encoding="utf-8") as csv_trg_file:
                    csv_reader = csv.reader(csv_trg_file)

                    # skip header row
                    next(csv_reader, None)
                    prop_lengths.append(sum(1 for row in csv_reader))
            except Exception as e:
                print(str(e))
                continue
    
    print(max(prop_lengths))

    fig, ax = plt.subplots()

    ax.hist(prop_lengths,  histtype="bar", bins=range(1,101), color="maroon")
    ax.set_xlabel("Entity count (values above 100 are accumulated)")
    ax.set_ylabel("Properties")

    fig.savefig("prop_dist.png", dpi=300, bbox_inches="tight")


def get_lang_overlap(translations: set, lang_codes: list) -> dict:
    """get the overlap between the three languages"""
    de_len = 0
    en_len = 0
    nl_len = 0

    de_nl_len = 0
    de_en_len = 0
    nl_en_len = 0

    de_nl_en_len = 0

    de_id = lang_codes.index("de")
    nl_id = lang_codes.index("nl")
    en_id = lang_codes.index("en")

    with tqdm(total=len(translations)) as pbar:
        for translation in translations:

            pbar.update(1)
            if translation[de_id] != "" and translation[nl_id] != "" and translation[en_id] != "":
                de_len += 1
                nl_len += 1
                en_len += 1
                de_nl_len += 1
                de_en_len += 1
                nl_en_len += 1
                de_nl_en_len += 1
                continue

            if translation[de_id] != "" and translation[nl_id] != "":
                de_len += 1
                nl_len += 1
                de_nl_len += 1
                continue

            if translation[de_id] != "" and translation[en_id] != "":
                de_len += 1
                en_len += 1
                de_en_len += 1
                continue

            if translation[nl_id] != "" and translation[en_id] != "":
                nl_len += 1
                en_len += 1
                nl_en_len += 1
                continue

            if translation[nl_id] != "":
                nl_len += 1
                continue

            if translation[en_id] != "":
                en_len += 1
                continue

            if translation[de_id] != "":
                de_len += 1

    return {
        "de": de_len,
        "nl": nl_len,
        "en": en_len,
        "de_nl": de_nl_len,
        "de_en": de_en_len,
        "nl_en": nl_en_len,
        "all": de_nl_en_len
    }