from bigquery_utils import write_json_gcs, load_gcs_json_bigquery
from data_api import ApiClient
from typing import List, Dict
from google.cloud import bigquery


PROJECT = "gcp-stl"
DATASET = "da_de_bootcamp_2022"
TABLE = "dog_images"
PARTITION = "2022010112"
GCS_PATH = "gs://da_de_bootcamp_2022/carolina_scalzo/dog_images.json"
API_URL = "https://dog.ceo/api/breeds/image/random"


def dog_images(data: dict) -> List[Dict]:
    """Specific transforms for the dogimages API data
    
    Args:
        data - a data dict from a response
    
    Returns:
        a dict transformed to the correct schema for imgflip data
    """
    records = data[""]

    return records


def process(
    url: str,
    gcs_path: str,
    project_id: str = PROJECT,
    dataset: str = DATASET,
    table: str = TABLE,
    partition: str = None,
) -> bigquery.job.load.LoadJob:
    """Runs the full ingest process for imgflip memes data
    
    Args:
        url - the API url
        gcs_path - the location in Cloud Storage where you want to dump the data
        project_id - your GCP project
        dataset - name of dataset for target BQ table
        table - name of BQ table
        partition (optional) - partition value for target table

    Returns:
        A Bigquery load job
    """
    client = ApiClient(url)
    data = client.get_data()
    records = dog_images(data)

    write_json_gcs(data=records, gcs_path=gcs_path)

    job = load_gcs_json_bigquery(
        gcs_data_path=gcs_path,
        project_id=project_id,
        dataset=dataset,
        table=table,
        partition=partition,
    )

    return job.result()


if __name__ == "__main__":

    result = process(url=API_URL, gcs_path=GCS_PATH, partition=PARTITION)
    print(result)

