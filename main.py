from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path
from yaml import load, SafeLoader, dump
from psycopg2 import connect
from contextlib import contextmanager
from typing import Generator


@dataclass
class Args:
    filter_yaml_file: Path
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
    args = parser.parse_args()
    return Args(
        filter_yaml_file=args.filter_yaml_file,
        hostname=args.hostname,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
    )


@contextmanager
def _connect_to_database(hostname: str, port: int, database: str, user: str, password: str) -> Generator:
    connection = connect(
        host=hostname, port=port, dbname=database, user=user, password=password
    )
    cursor = connection.cursor()
    yield cursor
    cursor.close()
    connection.close()


def _main() -> None:
    args = _args()
    print(args.filter_yaml_file)
    with open(args.filter_yaml_file, "r") as file_pointer:
        filter_dict = load(file_pointer, Loader=SafeLoader)
        print(filter_dict["publications"][0])

    table = "fulltext"

    with _connect_to_database() as cursor:
        cursor.execute(f"SELECT * FROM {table}")
        print(cursor.fetchone())


if __name__ == "__main__":
    _main()
