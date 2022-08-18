import gc
from boto import config
import gcsfs
from google.cloud import bigquery
import json
from typing import List, Dict

from sqlalchemy import table


def json_dumps_newline(data: List[Dict]) -> str:
    """Outputs a newline-delimited JSON string"""
    records = [json.dumps(record) for record in data]

    return '\n'.join(records)


def write_json_gcs(data: dict, gcs_path: str) -> None:
    """Writes a dictionary to GCS in the correct format for BigQuery
    
    Args:
        data - a dictionary that can be written as json
        gcs_path - the location to write the data
    """
    client = gcsfs.GCSFileSystem()

    with client.open(gcs_path, 'w') as f:
        f.write(json_dumps_newline(data))


def load_gcs_json_bigquery(gcs_data_path: str, project_id: str, dataset: str, table: str, partition: str = None):
    """Loads a JSON file from GCS into a BigQuery table
    Args:
        gcs_data_path - gs:// location of data to load. Must be newline-delimited JSON
        project_id - project for BigQuery table
        dataset - dataset for BigQuery table
        table - name of target table
        partition (optional) - partition ID

    """
    destination_dataset_table = f"{project_id}.{dataset}.{table}"
    if partition is not None:
        destination_dataset_table += f"${partition}"

    client = bigquery.Client()

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    load_job = client.load_table_from_uri(
        source_uris=gcs_data_path,
        destination=destination_dataset_table,
        project=project_id,
        job_config=config
    )

    return load_job.result()
    
