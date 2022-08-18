from bigquery_utils import write_json_gcs, load_gcs_json_bigquery
from data_api import ApiClient
from typing import List, Dict


PROJECT = "gcp-stl"
DATASET = "da_de_bootcamp_2022"
TABLE = "imgflip_memes"
GCS_PATH = "gs://da_de_bootcamp_2022/stephen_wallace/imgflip_memes.json"
API_URL = "https://api.imgflip.com/get_memes"


def img_flip_transform(data: dict) -> List[Dict]:
    """Specific transforms for the imgflip API data
    
    Args:
        data - a data dict from a response
    
    Returns:
        a dict transformed to the correct schema for imgflip data
    """
    records = data["memes"]

    return records


def process(
    url: str,
    gcs_path: str,
    project_id: str = PROJECT,
    dataset: str = DATASET,
    table: str = TABLE,
    partition: str = None,
):
    """Runs the full ingest process for imgflip memes data
    
    
    """
    client = ApiClient(url)

    data = client.process()

    records = img_flip_transform(data)

    write_json_gcs(data=records, gcs_path=gcs_path)

    job = load_gcs_json_bigquery(
        gcs_data_path=gcs_path,
        project_id=project_id,
        dataset=dataset,
        table=table,
        partition=partition,
    )

    return job


if __name__ == "__main__":
    
    job = process(url=API_URL, gcs_path=GCS_PATH)

