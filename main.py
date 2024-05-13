from argparse import ArgumentParser
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

from psycopg2 import connect
from psycopg2.extensions import cursor
from yaml import SafeLoader, dump, load
from tqdm import tqdm


@dataclass
class Args:
    filter_yaml_file: Path
    output_dir: Path
    hostname: str
    port: int
    database: str
    user: str
    password: str


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
    args = parser.parse_args()
    return Args(
        filter_yaml_file=args.filter_yaml_file,
        output_dir=args.output_dir,
        hostname=args.hostname,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
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


def _fetch_fulltext_hash(database_cursor: cursor, domain: str) -> list[str]:
    warcinfo_table = "warcinfo"
    database_cursor.execute(
        f"SELECT fulltext_hash FROM {warcinfo_table} WHERE domain = '{domain}'"
    )
    all_results = database_cursor.fetchall()
    filtered_results = []
    for result in all_results:
        if len(result) > 1:
            raise ValueError("More than one result returned")
        if result[0] is not None:
            if result[0] not in filtered_results:
                filtered_results.append(result[0])
    return filtered_results


def _main() -> None:
    args = _args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    responsible_editor_key = "have-responsible-editor"
    domain_key = "domain"
    title_key = "title"

    with _connect_to_database(
        hostname=args.hostname,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
    ) as database_cursor:
        with open(args.filter_yaml_file, "r", encoding="utf-8") as file_pointer:
            filter_dict = load(file_pointer, Loader=SafeLoader)
            for items in tqdm(filter_dict["publications"]):
                print(f"Processing domain {items[domain_key]}", flush=True)
                if not items[responsible_editor_key]:
                    raise ValueError("No responsible editor")
                result = _fetch_fulltext_hash(database_cursor, items[domain_key])
                fulltext_for_domain_dict = {}
                print(f"Found {len(result)} fulltext hashes", flush=True)
                for fulltext_hash in tqdm(result):
                    fulltext_for_domain_dict[fulltext_hash] = (
                        _fetch_fulltext_with_fulltext_hash(
                            database_cursor, fulltext_hash
                        )
                    )

                fulltext_dict = {
                    title_key: items[title_key],
                    domain_key: items[domain_key],
                    responsible_editor_key: items[responsible_editor_key],
                    "fulltexts": fulltext_for_domain_dict,
                }

                with open(
                    args.output_dir / f"{items[domain_key]}.yaml", "w", encoding="utf-8"
                ) as file_pointer:
                    dump(
                        fulltext_dict, file_pointer, allow_unicode=True, sort_keys=False
                    )


if __name__ == "__main__":
    _main()
