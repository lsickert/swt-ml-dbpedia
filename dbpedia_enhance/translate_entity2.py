import requests


def main():
    uri = "University of Groningen"
    language = "en"

    URL = "https://" + language + ".wikipedia.org/w/api.php"

    PARAMS = {
        "action": "query",
        "titles": uri,
        "prop": "langlinks",
        "lllimit": "500",
        "formatversion": "2",
        "format": "json"
    }

    r = requests.get(url = URL, params=PARAMS)
        # "https://en.wikipedia.org/w/api.php?action=query&titles=Michael_Jackson_(writer)&prop=langlinks&lllimit=500&formatversion=2&format=json")

    data = r.json()

    results = {"en": [], "nl": [], "de": []}
    results[language].append(uri)

    for item in data['query']['pages']:
        for language_info in item['langlinks']:
            if (language_info['lang'] == 'en'):
                results['en'].append(language_info['title'])
            if (language_info['lang'] == 'nl'):
                results['nl'].append(language_info['title'])
            if (language_info['lang'] == 'de'):
                results['de'].append(language_info['title'])
            print(language_info['lang'] + '\t' + language_info['title'])
    print(results)

if __name__ == "__main__":
    main()
