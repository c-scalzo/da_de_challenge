from src.bigquery_utils import json_dumps_newline, write_json_gcs


BUCKET = "da_de_bootcamp_2022"
PREFIX = "stephen_wallace/json_test.json"


def test_json_dumps_newline(test_data):
    """Ensure json is formatted as expected """
    act_val = '{"one": 1, "two": "A"}\n{"one": 2, "two": "B"}'
    test_val = json_dumps_newline(test_data)

    assert act_val == test_val


def test_write_json_gcs(test_data, get_filesystem):
    """Confirm that function writes data as expected"""
    gcs_path = f"{BUCKET}/{PREFIX}"
    write_json_gcs(test_data, gcs_path)

    with get_filesystem.open(gcs_path, "r") as f:
        act_data = f.read()

    assert '{"one": 1, "two": "A"}\n{"one": 2, "two": "B"}' == act_data