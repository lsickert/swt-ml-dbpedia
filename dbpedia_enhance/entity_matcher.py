import os
import os.path
import csv

def find_entity_match(property, src_lang, trg_lang):
    """ find an entity with a given property in one language that also exists in another language."""
    for filename in os.listdir(src_lang):
        with open('{}/{}'.format(src_lang, filename), encoding='utf-8') as f:
            srcreader = csv.reader(f)
            for row in srcreader[1:]:
                if row[0] == property:
                    if os.path.isfile('{}/{}'.format(trg_lang, filename)):
                        print('Match:', property, filename)

                    """the following code is a start at opening the matching target file and looking for the property there."""
                    #try:
                    #    with open('{}/{}.csv'.format(trg_lang, filename), encoding='utf-8') as g:
                    #        trgreader = csv.reader(g)
                    #        for row in trgreader:
                    #            if row[0] == property:
                    #                prop_match = True

def find_matching_prop(entity, src_lang, trg_lang):
    # """ for a given entity, find properties with matching values in both languages."""

        with open('data/{}/{}.csv'.format(src_lang, entity), encoding='utf-8') as f:
            srcreader = csv.reader(f)
            prop_matches = []
            for src_row in srcreader:


                try:
                    with open('data/{}/{}.csv'.format(trg_lang, entity), encoding='utf-8') as g:
                        trgreader = csv.reader(g)
                        for trg_row in trgreader:

                            if src_row[1] == trg_row[1]:
                                prop_matches.append([src_row[0], trg_row[0], src_row[1]])
                except:
                    pass
        print(entity, '\n', prop_matches)

#find_entity_match('website', 'en', 'nl')
find_matching_prop('!!!', 'en', 'nl')