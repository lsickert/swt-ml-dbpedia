import argparse
import re
import data.utils as dat_util
from dbpedia_enhance import property_extractor, entity_extractor_new, property_matcher

parser = argparse.ArgumentParser(prog="DBpedia Property Enhancer",
                                 description="This program will enhance dbpedia coverage by bidirectionally matching missing properties between two languages.")

parser.add_argument("--src_lang", type=str, default="en",
                    help="The source language from where properties should be extracted")

parser.add_argument("--trg_lang", type=str, required=True,
                    help="The target language from where properties should be extracted")

parser.add_argument("--version", type=str, default="2022.03.01",
                    help="The version of the DBpedia infobox dump to use for analysis")
parser.add_argument("--force_new", type=bool, default=False,
                    help="Force a regeneration of all extracted properties")

ALL_LANG_FILES = [
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=de.ttl.bz2",
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=en.ttl.bz2",
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=nl.ttl.bz2"
]

if __name__ == "__main__":

    options = parser.parse_args()

    src_lang_link = f"https://databus.dbpedia.org/dbpedia/generic/infobox-properties/{options.version}/infobox-properties_lang={options.src_lang}.ttl.bz2"
    trg_lang_link = f"https://databus.dbpedia.org/dbpedia/generic/infobox-properties/{options.version}/infobox-properties_lang={options.trg_lang}.ttl.bz2"

    lang_files = [src_lang_link, trg_lang_link]

    #replace this with ALL_LANG_FILES to run download for all considered languages
    filenames = dat_util.get_data(lang_files)

    #TODO: write this in a better way
    src_props = set()
    src_entities = set()
    trg_props = set()
    trg_entities = set()
    for fname in filenames:
        if re.search(f"{options.src_lang}.ttl", fname):
            src_props = property_extractor.extract_properties(fname)
            #src_entities = entity_extractor_new.extract_subjects(fname)
        else:
            trg_props = property_extractor.extract_properties(fname)
            #trg_entities = entity_extractor_new.extract_subjects(fname)

    #print(property_matcher.find_direct_matches(src_props, trg_props))

    print(property_matcher.find_entity_matches(list(src_props), list(trg_props), options.src_lang, options.trg_lang))