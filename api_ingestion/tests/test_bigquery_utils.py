from sqlalchemy import table
from src.bigquery_utils import json_dumps_newline, write_json_gcs, load_gcs_json_bigquery
import pandas as pd


BUCKET = "da_de_bootcamp_2022"
PREFIX = "carolina_scalzo/json_test.json"
PROJECT_ID = 'gcp-stl'
DATASET = "da_de_bootcamp_2022"
TABLE = "test_bigquery_utils"

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


def test_load_gcs_json_bigquery(test_data):
    """ """
    gcs_path = f"gs://{BUCKET}/{PREFIX}"
    test_result = load_gcs_json_bigquery(
        gcs_data_path=gcs_path, 
        project_id=PROJECT_ID, 
        dataset=DATASET, 
        table=TABLE
    )

    assert test_result.errors is None
