from argparse import ArgumentParser
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator

from nb_tokenizer import tokenize
from psycopg2 import connect
from psycopg2.extensions import cursor
from tqdm import tqdm
from yaml import SafeLoader, dump, load
import jsonlines
import sqlite3
import os
import sys
import uuid


@dataclass
class _DHlabIdStatus:
    is_disabled: bool
    starting_value: int


@dataclass
class _DatabaseArgs:
    hostname: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class _FulltextMetadata:
    record_id: str
    warcpath: Path
    hash: str
    uri: str
    timestamp: str


@dataclass
class Args:
    filter_yaml_file: Path
    output_dir: Path
    dhlab_id_status: _DHlabIdStatus
    database: _DatabaseArgs


@dataclass
class _TokenParseResult:
    token: str
    sequence_number: int
    paragraph_number: int


def _args() -> Args:
    parser = ArgumentParser()
    parser.add_argument(
        "--filter-yaml-file",
        type=Path,
        required=True,
        help="Path to the filter yaml file",
    )
    parser.add_argument(
        "--hostname", type=str, required=True, help="Hostname of the database"
    )
    parser.add_argument("--port", type=int, required=True, help="Port of the database")
    parser.add_argument("--database", type=str, required=True, help="Database name")
    parser.add_argument(
        "--user", type=str, required=True, help="Username of the database"
    )
    parser.add_argument(
        "--password", type=str, required=True, help="Password of the database"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Path to the output dir",
    )
    dhlab_id_group = parser.add_mutually_exclusive_group(required=True)
    dhlab_id_group.add_argument(
        "--starting-dhlab-id", type=int, help="Starting dhlab id"
    )
    dhlab_id_group.add_argument(
        "--disable-dhlab-id", action="store_true", help="Disable dhlab id"
    )
    args = parser.parse_args()
    return Args(
        filter_yaml_file=args.filter_yaml_file,
        output_dir=args.output_dir,
        database=_DatabaseArgs(
            hostname=args.hostname,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
        ),
        dhlab_id_status=_DHlabIdStatus(
            is_disabled=args.disable_dhlab_id,
            starting_value=args.starting_dhlab_id if not args.disable_dhlab_id else 0,
        ),
    )


@contextmanager
def _connect_to_database(
    hostname: str, port: int, database: str, user: str, password: str
) -> Generator:
    connection = connect(
        host=hostname, port=port, dbname=database, user=user, password=password
    )
    database_cursor = connection.cursor()
    yield database_cursor
    database_cursor.close()
    connection.close()

def _create_local_db(dbname):
    if os.path.exists(dbname):
        print("ERROR: Database already exists. Delete and re-run.")
        sys.exit(1)
    with sqlite3.connect(dbname) as dbcon:
        cur = dbcon.cursor()
        cur.execute("CREATE TABLE urns (urn INTEGER PRIMARY KEY, urntext text);")
        cur.execute("CREATE TABLE ft (urn int, word varchar, seq int, para int, page int, ordinal int);")
        cur.execute("CREATE TABLE metadata (dhlabid int, hash text, title text, domain text, responsible_editor bool, place text, county text, record_id text, warcpath text, timestamp text, uri text);")

def _write_to_local_database(dbname, token_tuples, metadata_tuple):
    with sqlite3.connect(dbname) as dbcon:
        cur = dbcon.cursor()
        cur.execute("INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", (metadata_tuple))
        dbcon.commit()
        cur.execute("INSERT INTO urns VALUES (?, ?);", (metadata_tuple[0], f"{metadata_tuple[8]}#{metadata_tuple[7]}"))
        dbcon.commit()
        cur.executemany("INSERT INTO ft(urn, word, seq, para) VALUES(?, ?, ?, ?);", (token_tuples))
        dbcon.commit()

def _rename_db(dbname, output_dir):
    # first we get the min and max_id
    with sqlite3.connect(dbname) as dbcon:
        cur = dbcon.cursor()
        cur.execute("SELECT min(urn) as urn_min, max(urn) as urn_max FROM urns;")
        min_max = cur.fetchone()

    new_dbname = f"{output_dir}/alto_{min_max[0]}_{min_max[1]}.db"
    os.rename(dbname, new_dbname)

def _remove_duplicates_and_empty_strings(results: list[tuple[str, ...]]) -> list[str]:
    filtered_results = []
    for entry in results:
        for item in entry:
            if not item == "":
                if item not in filtered_results:
                    filtered_results.append(item)
    return filtered_results


def _fetch_fulltext_with_fulltext_hash(
    database_cursor: cursor, fulltext_hash: str
) -> list[str]:
    fulltext_table = "fulltext"
    database_cursor.execute(
        f"SELECT fulltext FROM {fulltext_table} WHERE fulltext_hash = '{fulltext_hash}'"
    )
    results = database_cursor.fetchall()
    return _remove_duplicates_and_empty_strings(results)


def _fetch_fulltext_hash_and_metadata(
    database_cursor: cursor, domain: str
) -> list[_FulltextMetadata]:
    warcinfo_table = "warcinfo"
    database_cursor.execute(
        f"SELECT DISTINCT ON (fulltext_hash, target_uri) record_id, wf.warc_file_name AS warcpath, fulltext_hash, target_uri, date FROM {warcinfo_table} w JOIN warc_files wf ON wf.warc_file_id = w.warc_file_id WHERE domain = '{domain}' AND fulltext_hash != 'da39a3ee5e6b4b0d3255bfef95601890afd80709' ORDER BY fulltext_hash, target_uri, date ASC;"
    )
    all_results = database_cursor.fetchall()
    filtered_results = []
    for result in all_results:
        if len(result) != 5:
            raise ValueError(
                f"Unexpected number of results, expected 5, got {len(result)}"
            )
        if result[0] is not None:
            if result[0] not in [metadata.hash for metadata in filtered_results]:
                parsed_date = datetime.fromisoformat(result[4].replace("Z", "+00:00"))
                formatted_date = parsed_date.strftime("%Y%m%d")
                res = _FulltextMetadata(
                    record_id=result[0], warcpath=result[1], hash=result[2], uri=result[3], timestamp=formatted_date
                )
                filtered_results.append(res)
    return filtered_results


def _parse_tokens(fulltext: str) -> list[_TokenParseResult]:
    result = []
    paragraphs = fulltext.split("\n")
    sequence_number = 0
    for paragraph_number, paragraph in enumerate(paragraphs):
        tokens = tokenize(paragraph)
        for token in tokens:
            result.append(
                _TokenParseResult(
                    token=token,
                    sequence_number=sequence_number,
                    paragraph_number=paragraph_number,
                )
            )
            sequence_number += 1

    return result


def _main() -> None:
    args = _args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    responsible_editor_key = "have-responsible-editor"
    domain_key = "domain"
    title_key = "title"
    hash_key = 'hash'
    geodata_key = "geodata"
    if not args.dhlab_id_status.is_disabled:
        dhlabid_value = args.dhlab_id_status.starting_value
    else:
        dhlabid_value = 1

    # initiate database
    dbid = str(uuid.uuid4())
    dbname = f"{args.output_dir}/{dbid}.db"
    _create_local_db(dbname)

    with _connect_to_database(
        hostname=args.database.hostname,
        port=args.database.port,
        database=args.database.database,
        user=args.database.user,
        password=args.database.password,
    ) as database_cursor:
        with open(args.filter_yaml_file, "r", encoding="utf-8") as file_pointer:
            filter_dict = load(file_pointer, Loader=SafeLoader)
            for items in tqdm(filter_dict["publications"]):
                try:
                    print(f"Processing domain {items[domain_key]}", flush=True)
                    if not items[responsible_editor_key]:
                        raise ValueError("No responsible editor")
                    fulltext_metadata_collection = _fetch_fulltext_hash_and_metadata(
                        database_cursor, items[domain_key]
                    )

                    print(
                        f"Found {len(fulltext_metadata_collection)} documents",
                        flush=True,
                    )

                    for fulltext_metadata in tqdm(fulltext_metadata_collection):
                        all_tokens_list = []

                        for full_text in _fetch_fulltext_with_fulltext_hash(
                            database_cursor=database_cursor,
                            fulltext_hash=fulltext_metadata.hash,
                        ):

                            warc_data =  {
                                    "record_id": fulltext_metadata.record_id,
                                    "warcpath": fulltext_metadata.warcpath,
                                    "timestamp": fulltext_metadata.timestamp,
                                    "uri": fulltext_metadata.uri,
                                    "full_text": full_text
                                }

                            fulltext_dict = {
                                'dhlabid': dhlabid_value,
                                hash_key: fulltext_metadata.hash,
                                title_key: items[title_key],
                                domain_key: items[domain_key],
                                responsible_editor_key: items[responsible_editor_key],
                                'place': items[geodata_key]['place'],
                                'county': items[geodata_key]['county']
                            }

                            fulltext_dict.update(warc_data)

                            metadata_tuple = tuple(fulltext_dict.values())[:-1]

                            with jsonlines.open(args.output_dir / f"{items[domain_key]}.yaml", "a") as writer:
                                writer.write(fulltext_dict)

                            # tokenize and output to sqlite3
                            token_result_collection = _parse_tokens(full_text)

                            token_tuples = []

                            for token_result in token_result_collection:
                                token_tuples.append((fulltext_dict["dhlabid"], token_result.token, token_result.sequence_number, token_result.paragraph_number))

                            _write_to_local_database(dbname, token_tuples, metadata_tuple)

                            dhlabid_value += 1
                except Exception as e:
                    print(items[domain_key], "failed with", e)
                    continue

    _rename_db(dbname, args.output_dir)

if __name__ == "__main__":
    _main()
