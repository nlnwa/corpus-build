from main import _remove_duplicates_and_empty_strings


def test_remove_duplicates_and_empty_strings():
    results = [
        ("", "test1", "test2", "test3"),
        ("", "test1", "test2", "test3"),
        ("", "test1", "test2", "test3"),
        ("", "test1", "test2", "test3"),
    ]
    assert _remove_duplicates_and_empty_strings(results) == ["test1", "test2", "test3"]
