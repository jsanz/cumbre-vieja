import os
import sys
import logging

from elasticsearch import Elasticsearch

from dotenv import load_dotenv

import footprints
import earthquakes

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)

load_dotenv()

# Create the client
ES_CLOUD_ID = os.getenv("ES_CLOUD_ID")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")

if not (ES_CLOUD_ID and ES_PASSWORD and ES_USER):
    logger.critical("Environment variables missing")
    sys.exit(1)

es_client = Elasticsearch(cloud_id=ES_CLOUD_ID, http_auth=(ES_USER, ES_PASSWORD))

# Footprints

# Create the footprints index
footprints.create_footprints_index(es_client)

# Download the geojson objects
features = footprints.download_footprints()
logger.info(f"Retrieved {len(features)} features from the Open Data portal")

# Process the footprints to get the differences
diffed_features = footprints.get_diffed_features(features)

# Upload to ES
logger.info("Indexing the footprints...")

fp_results = footprints.index_footprints(es_client, diffed_features, overwrite=False)
logger.info(f"indexed: {fp_results['indexed']}")
logger.info(f"skipped: {fp_results['skipped']}")
logger.info(f"errors:  {fp_results['errors']}")

logger.info("Footprints done!")

# Earthquakes
logger.info("Downloading quakes data...")
quakes = earthquakes.download_earthquakes()
logger.info(f"Retrieved {len(quakes)} quake entries, indexing...")
earthquakes.index_quakes(es_client, quakes)
logger.info("Quakes done!")

logger.info("Process finished")
