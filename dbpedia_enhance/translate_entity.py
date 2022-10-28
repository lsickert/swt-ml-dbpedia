import requests


def match_uri(text):
    url = 'https://www.wikidata.org/w/api.php'
    params = {
        'action': 'wbsearchentities',
        'language': 'en',
        'search': text,
        'limit': 7,
        'format': 'json'
    }
    data = requests.get(url, params=params).json()
    if data is None:
        URI = None
        return

    URIs = []
    for item in data['search']:
        URIs.append(str(item['id']))

    # print(URIs)
    return URIs


def get_translation_query(URIs, language_1, language_2):
    query = '''
    SELECT ?itemLabel_1 ?itemLabel_2 WHERE{
        wd:''' + URIs[0] + ''' rdfs:label ?itemLabel_1.FILTER(lang(?itemLabel_1)=''' + "'" + language_1 + "'" + ''')
        wd:''' + URIs[0] + ''' rdfs:label ?itemLabel_2.FILTER(lang(?itemLabel_2)=''' + "'" + language_2 + "'" + ''')
    }
    '''

    # print(query)
    return query


def run_query(query):
    url = 'https://query.wikidata.org/sparql'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
    try:
        data = requests.get(url, headers=headers, params={
                            'query': query, 'format': 'json'}).json()
    except ValueError:
        print("Received a value error from WikiData API")
        return None
    return data


def query_all(uri, l1, l2, results):
    query = get_translation_query(uri, l1, l2)
    data = run_query(query)
    # print(data)

    for item in data['results']['bindings']:
        # print(item['itemLabel_1']['value'])
        # print(item['itemLabel_2']['value'])
        if not results[l1]:
            results[l1].append(item['itemLabel_1']['value'])

        if not results[l2]:
            results[l2].append(item['itemLabel_2']['value'])
    return results


def translate_all(uri):
    language_1 = "en"
    language_2 = "nl"
    language_3 = "de"
    results = {"en": [], "nl": [], "de": []}

    results = query_all(uri, language_1, language_2, results)
    results = query_all(uri, language_1, language_3, results)
    results = query_all(uri, language_2, language_3, results)
    print(results)
    return results


def main():
    uri = match_uri("University of Groningen")
    translate_all(uri)


if __name__ == "__main__":
    main()
