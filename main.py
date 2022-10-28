import data.utils as dat_util
from dbpedia_enhance import property_extractor, entity_extractor_new, utils


LANG_FILES = [
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=de.ttl.bz2",
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=en.ttl.bz2",
    "https://databus.dbpedia.org/dbpedia/generic/infobox-properties/2022.03.01/infobox-properties_lang=nl.ttl.bz2"
]

if __name__ == "__main__":

    filenames = dat_util.get_data(LANG_FILES)

    lang_codes = []
    for fname in filenames:
        lang_codes.append(utils.get_lang_code(fname))
        property_extractor.extract_properties(fname)
        entity_extractor_new.extract_subjects(fname)

    
