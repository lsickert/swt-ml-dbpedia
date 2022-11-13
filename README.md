# Automatic Expansion of Wikipedia Infoboxes through Attribute Matching

This project aims to enhance the multilingual coverage of DBpedia by matching properties between different languages. It is a group projects as part of the Semantic Web Technology MSc course 2022 of the [University of Groningen](https://www.rug.nl/).

## Installation

All dependecies needed to run the module can be installed by running the following command:

```
pip3 install -r requirements.txt
```

## How To Run

The module can be started and configured from the command line with the following options:

|Option|Description|Default Value|
|------|-----------|-------------|
|`src_lang`|The source language from where properties should be extracted|en|
|`trg_lang`|The target language from where properties should be extracted| None **(required)** |
|`version`|The version of the DBpedia infobox dump to use for analysis|2022.03.01|
|`src_cat`|Limit the extraction of properties on the source file to members of this category|None|
|`trg_cat`|Limit the extraction of properties on the target file to members of this category|None|
|`out_suffix`|Add this as suffix to the names of the extracted files|None|
|`force_new`|Force a regeneration of all extracted properties|False|

An example run could look like this: `python main.py --src_lang en --trg_lang de --src_cat Category:Countries_in_Europe --trg_cat Kategorie:Staat_in_Europa --out_suffix country`
