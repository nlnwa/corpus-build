# corpus-build

This is a small repository for building a corpus of text, extracted from WARC-files in the Norwegian Web Archive's collection. The filtering dataset is based on a list of domains for Norwegian news websites which have declared responsible editorship, while tokenisation utilises the [DH-lab tokenizer](https://github.com/NationalLibraryOfNorway/DHLAB/blob/main/dhlab/text/nbtokenizer.py). 

Its main functions are:
- Reading fulltext from an internal database with natural language, extracted from WARC-files with `content-type:text/html`
- Filters for specific domains with a declared responsible editor
- Tokenise the text using DH-lab tokenizer
- Writes the tokenised text to .sqlite databases

At the moment, the scope of data is limited to the years 2018-2022.

## Getting started




## Requirements

### Python-libraries:
- argparse
- dataclasses
- pathlib
- yaml
- psycopg2
- contextlib
- typing
- dhlab