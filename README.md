# Cumbre Vieja data publishing

Data workflow to publish geospatial information related to La Palma Cumbre Vieja volcano eruption into Elasticsearch.

How to run the data script:

1. Create a Python virtual environment and install dependencies with `pip install -r requirements.txt`
2. Export the environment variables: `ES_CLOUD_ID`, `ES_USER`, and `ES_PASSWORD` or alternatively, store them in an `.env` file
3. Run `python src/app.py`

Notes:

* This repo has a Github Actions [worflow](https://github.com/jsanz/cumbre-vieja/blob/main/.github/workflows/python-app.yml) to run the process on every push to the `main` branch.
* Adapt the Elasticsearch Python `client` initialization if you use a different authentication than an Elastic Cloud identifier.
* Check the `app.py` script for boolean variables to control which data to process and if you want to export the datasets into the `/tmp` folder as GeoJSON files.
* HTTP requests are cached in a SQLite database stored in the user cache directory (`$USER/.cache` in Linux systems).
