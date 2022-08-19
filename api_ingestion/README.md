# API Integestion Challenge

## Objectives
1. Find a public data api of your choice. Some examples can be found here: https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/

2. Write a process using a scripting language (Python, bash, etc.) to make a request to the API endpoint and extract the data from the response.

3. Perform whatever cleaning or transforming steps you feel necessary to the data

4. Load the data to BigQuery. You can either dump the data to Cloud Storage first as JSON or load directly to BigQuery from your script.


## Setup
1. If you haven't already, you will need to install the gcloud command line:
https://cloud.google.com/sdk/docs/install-sdk#installing_the_latest_version

and initialize it:
https://cloud.google.com/sdk/docs/install-sdk#initializing_the


2. Authenticate to Google Cloud. In your terminal run:  
`gcloud auth application-default login`  
and follow the steps.

3. Install the Google Cloud Python libraries:
google-cloud-bigquery
google-cloud-storage

## Stuck?
This repo contains an example solution. This isn't the only right way to solve this problem, but it works.