import logging
import warnings

from data import download_geojson

from elasticsearch.helpers import bulk
from elasticsearch.client import IndicesClient
from elasticsearch.client.enrich import EnrichClient
from elasticsearch.client.ingest import IngestClient
from elasticsearch.exceptions import TransportError
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import RequestError

from shapely.geometry import shape, mapping
from area import area

warnings.filterwarnings("ignore")
logging.getLogger("elasticsearch").setLevel(logging.ERROR)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")

INDEX_NAME = "lapalma_buildings"
GEOJSON_URL = (
    "https://opendata.arcgis.com/datasets/1c93601970fb41b480599c54fff25e4f_0.geojson"
)


def get_actions(features):
    for feature in features:
        try:
            properties = feature["properties"]
            geometry = feature["geometry"]

            id = properties["OBJECTID"]
            s_geom = shape(geometry).buffer(0)
            centroid = mapping(s_geom.centroid)
            gejoson_geom = mapping(s_geom)
            geom_area = int(area(gejoson_geom))

            if geom_area > 0:
                doc = {
                    "id": id,
                    "geometry": gejoson_geom,
                    "centroid": centroid,
                    "area": geom_area,
                    "level": properties["LEVEL_"],
                    "name": properties["LNAME"],
                    "floors": properties["NUM_PLANTA"],
                }
                yield {
                    "_index": INDEX_NAME,
                    "_op_type": "index",
                    "_id": str(id),
                    "_source": doc,
                }
        except Exception as e:
            logger.error(f"[{type(e)}] - {e}")


def create_index(client, index_name):
    try:
        # Create the index
        client.indices.create(
            index=index_name,
            settings={"number_of_shards": 1, "number_of_replicas": 1},
            mappings={
                "properties": {
                    "id": {"type": "integer"},
                    "geometry": {"type": "geo_shape"},
                    "centroid": {"type": "geo_shape"},
                    "area": {"type": "integer"},
                    "level": {"type": "integer"},
                    "name": {"type": "keyword"},
                    "floors": {"type": "integer"},
                }
            },
        )
        # Create the alias
        IndicesClient(client).put_alias(index_name, f"all_{index_name}")
    except RequestError:
        logger.info("Index already exists, continuing")


def create_policy(client):
    enrich_client = EnrichClient(client)
    try:
        policy = enrich_client.get_policy(name="lapalma_lookup")
        if len(policy['policies']) == 0:
            raise NotFoundError
    except NotFoundError:
        logger.debug("Creating the lapalma_lookup policy")
        enrich_client.put_policy(
            name="lapalma_lookup",
            body={
                "geo_match": {
                    "indices": "lapalma",
                    "match_field": "diff_geometry",
                    "enrich_fields": ["id", "timestamp"],
                }
            },
        )

    # Execute the policy
    logger.info("Updating the enrich policy...")

    try:
        enrich_client.execute_policy(name="lapalma_lookup")
    except TransportError as e:
        logger.error(e)

    logger.info("Done!")


def create_ingest_pipeline(ingest_client):
    logger.debug("Creating the enrich pipeline")
    ingest_client.put_pipeline(
        id="buildings_footprints",
        body={
            "description": "Enrich buildings with Cumbre Vieja footprints.",
            "processors": [
                {
                    "enrich": {
                        "field": "geometry",
                        "policy_name": "lapalma_lookup",
                        "target_field": "footprints",
                        "shape_relation": "INTERSECTS",
                        "ignore_missing": True,
                        "ignore_failure": True,
                    }
                },
                {
                    "remove": {
                        "field": "footprints.diff_geometry",
                        "ignore_missing": True,
                        "ignore_failure": True,
                        "description": "Remove the shape field",
                    }
                },
            ],
        },
    )


def index_buildings(client, overwrite=False):
    """
    Creates and populates an index with the buildings
    """

    # Ensure the policy exists an it's updated
    create_policy(client)

    # Ensure the pipeline exists
    ingest_client = IngestClient(client)
    try:
        ingest_client.get_pipeline(id="buildings_footprints")
    except NotFoundError:
        create_ingest_pipeline(ingest_client)

    # Create or overwrite the index
    exists = IndicesClient(client).exists(INDEX_NAME)

    if exists and overwrite:
        client.indices.delete(index=INDEX_NAME)

    if not exists or overwrite:
        create_index(client, INDEX_NAME)

        logger.info("Getting the buildings data...")
        features = download_geojson(GEOJSON_URL)
        logger.debug(f"{len(features)} buildings downloaded")

        # Bulk upload the records
        logger.info(f"Uploading to ES {len(features)} records...")
        bulk(client, get_actions(features))

    # Just reindex buildings inside the bouinding box without a footpirnt ID
    update_query = {
        "query": {
            "bool": {
                "must_not": [{"exists": {"field": "footprints.id"}}],
                "filter": {
                    "geo_bounding_box": {
                        "geometry": {
                            "top_left": {"lat": 28.647, "lon": -17.95},
                            "bottom_right": {"lat": 28.58, "lon": -17.83},
                        }
                    }
                },
            }
        }
    }

    count_obj = client.count(body=update_query, index=INDEX_NAME)
    if "count" in count_obj:
        count = count_obj["count"]
        if count > 0:
            logger.info(f"Reindexing {count} buildings without a footprint id...")
            client.update_by_query(
                INDEX_NAME,
                body=update_query,
                pipeline="buildings_footprints",
                wait_for_completion=False
            )
            logger.info("Update query sent without waiting for completion")
        else:
            logger.info("No buildings to update")
