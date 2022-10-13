import multiprocessing as mp

def find_direct_match(src_props: set, trg_props: set) -> set:
    """ find all properties in two sets where the property names are equal"""
    return set.intersection(src_props, trg_props)