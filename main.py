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
    hash: str
    uri: str
    timestamp: str


@dataclass
class Args:
    filter_yaml_file: Path
    output_dir: Path
    dhlab_id_status: _DHlabIdStatus
    database: _DatabaseArgs


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
        f"SELECT fulltext_hash, target_uri, date FROM {warcinfo_table} WHERE domain = '{domain}'"
    )
    all_results = database_cursor.fetchall()
    filtered_results = []
    for result in all_results:
        if len(result) != 3:
            raise ValueError(
                f"Unexpected number of results, expected 3, got {len(result)}"
            )
        if result[0] is not None:
            if result[0] not in [metadata.hash for metadata in filtered_results]:
                parsed_date = datetime.fromisoformat(result[2].replace("Z", "+00:00"))
                formatted_date = parsed_date.strftime("%Y%m%d")
                res = _FulltextMetadata(
                    hash=result[0], uri=result[1], timestamp=formatted_date
                )
                filtered_results.append(res)
    return filtered_results


def _main() -> None:
    args = _args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    responsible_editor_key = "have-responsible-editor"
    domain_key = "domain"
    title_key = "title"
    if not args.dhlab_id_status.is_disabled:
        dhlabid_value = args.dhlab_id_status.starting_value

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
                print(f"Processing domain {items[domain_key]}", flush=True)
                if not items[responsible_editor_key]:
                    raise ValueError("No responsible editor")
                fulltext_metadata_collection = _fetch_fulltext_hash_and_metadata(
                    database_cursor, items[domain_key]
                )
                fulltext_for_domain_dict = {}
                print(
                    f"Found {len(fulltext_metadata_collection)} fulltext hashes",
                    flush=True,
                )
                for fulltext_metadata in tqdm(fulltext_metadata_collection):
                    fulltext_for_domain_dict[fulltext_metadata.hash] = {
                        "text": list(
                            map(
                                tokenize,
                                _fetch_fulltext_with_fulltext_hash(
                                    database_cursor, fulltext_metadata.hash
                                ),
                            )
                        ),
                        "timestamp": fulltext_metadata.timestamp,
                        "uri": fulltext_metadata.uri,
                    }
                    if not args.dhlab_id_status.is_disabled:
                        fulltext_for_domain_dict[fulltext_metadata.hash][
                            "dhlab-id"
                        ] = dhlabid_value
                        dhlabid_value += 1

                fulltext_dict = {
                    title_key: items[title_key],
                    domain_key: items[domain_key],
                    responsible_editor_key: items[responsible_editor_key],
                    "text-entry": fulltext_for_domain_dict,
                }

                with open(
                    args.output_dir / f"{items[domain_key]}.yaml", "w", encoding="utf-8"
                ) as file_pointer:
                    dump(
                        fulltext_dict, file_pointer, allow_unicode=True, sort_keys=False
                    )


if __name__ == "__main__":
    _main()
