import argparse
import re
import data.utils as dat_util
from dbpedia_enhance import property_extractor, entity_extractor_new, property_matcher, translate_entity_new
from analysis import analysis

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
parser.add_argument("--src_cat", type=str, default=None,
                    help="Limit the extraction of properties on the source file to members of this category.")
parser.add_argument("--trg_cat", type=str, default=None,
                    help="Limit the extraction of properties on the source file to members of this category.")
parser.add_argument("--out_suffix", type=str, default=None,
                    help="Add this as suffix to the names of the extracted files")

ALL_LANG_FILES = [
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=de.ttl.bz2",
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=en.ttl.bz2",
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=nl.ttl.bz2"
]

# if __name__ == "__main__":

#     props = property_extractor.extract_properties("infobox-properties_lang=de.ttl")

#     analysis.get_prop_distribution(props, "de")

# if __name__ == "__main__":

#     lang_files = [
#         "infobox-properties_lang=de.ttl",
#         "infobox-properties_lang=en.ttl",
#         "infobox-properties_lang=nl.ttl"
#     ]

#     lang_codes, translations = translate_entity_new.get_translations(lang_files)

#     print(analysis.get_lang_overlap(translations, lang_codes))

if __name__ == "__main__":

    options = parser.parse_args()

    src_lang_link = f"https://databus.dbpedia.org/dbpedia/generic/infobox-properties/{options.version}/infobox-properties_lang={options.src_lang}.ttl.bz2"
    trg_lang_link = f"https://databus.dbpedia.org/dbpedia/generic/infobox-properties/{options.version}/infobox-properties_lang={options.trg_lang}.ttl.bz2"

    lang_files = [src_lang_link, trg_lang_link]

    # replace this with ALL_LANG_FILES to run download for all considered languages
    filenames = dat_util.get_data(lang_files)

    #subj_translations = translate_entity_new.get_translations(
    #    filenames, options.out_suffix)

    # TODO: write this in a better way
    src_props = set()
    src_entities = set()
    trg_props = set()
    trg_entities = set()
    for fname in filenames:
        if re.search(f"{options.src_lang}.ttl", fname):
            src_props = property_extractor.extract_properties(
                fname, options.out_suffix, [options.src_cat])
            src_entities = entity_extractor_new.extract_subjects(
                fname, options.out_suffix, [options.src_cat])
        else:
            trg_props = property_extractor.extract_properties(
                fname, options.out_suffix, [options.trg_cat])
            trg_entities = entity_extractor_new.extract_subjects(
                fname, options.out_suffix, [options.trg_cat])

    matches = property_matcher.find_matches(
        src_props, trg_props, options.src_lang, options.trg_lang, options.out_suffix)
    
    print("")
    print("#############")
    print(f"{len(matches)} matches found, {round(len(matches)/len(trg_props),4) * 100} percent of target properties matched")
    print("matches:")
    print(matches)

    #property_extractor.extract_properties("infobox-properties_lang=de.ttl","country","Kategorie:Staat in Europa")
    #entity_extractor_new.extract_subjects("infobox-properties_lang=de.ttl","country","Kategorie:Staat in Europa")