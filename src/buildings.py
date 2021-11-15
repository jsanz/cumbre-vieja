import logging

import requests_cache

from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError
from elasticsearch.client.enrich import EnrichClient
from elasticsearch.client.ingest import IngestClient

from shapely.geometry import shape, mapping
from area import area

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")

session = requests_cache.CachedSession("http_cache", use_cache_dir=True)

INDEX_NAME = "lapalma_buildings"
GEOJSON_URL = (
    "https://opendata.arcgis.com/datasets/1c93601970fb41b480599c54fff25e4f_0.geojson"
)


def download_buildings():
    """
    Get the buildings data for La Palma
    """
    r = session.get(GEOJSON_URL)
    if r.status_code != 200:
        raise Exception("Error downloading the buildings GeoJSON")

    r_obj = r.json()

    if "features" not in r_obj:
        raise Exception(f"Returned JSON is not a valid GeoJSON: [{r_obj.keys()}]")

    return r_obj["features"]


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
                    "pipeline": "buildings_footprints",
                    "_source": doc,
                }
        except Exception as e:
            logger.error(f"[{type(e)}] - {e}")


def index_buildings(client, features, overwrite=False):
    """
    Creates and populates an index with the buildings
    """

    # Ensure the policy exists an it's updated
    enrich_client = EnrichClient(client)
    try:
        enrich_client.get_policy(name="lapalma_lookup")
    except NotFoundError:
        logger.debug("Creating the lapalma_lookup policy")
        enrich_client.put_policy(
            name="lapalma_lookup",
            params={
                "geo_match": {
                    "indices": "lapalma",
                    "match_field": "diff_geometry",
                    "enrich_fields": ["id", "timestamp"],
                }
            },
        )

    # Execute the policy
    logger.info("Updating the enrich policy...")
    enrich_client.execute_policy(name="lapalma_lookup")
    logger.info("Done!")

    # Ensure the pipeline exists
    ingest_client = IngestClient(client)
    try:
        ingest_client.get_pipeline(id="buildings_footprints")
    except NotFoundError:
        logger.debug("Creating the enrich pipeline")
        ingest_client.put_pipeline(
            id="buildings_footprints",
            params={
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

    if overwrite:
        # Create the index if absent
        try:
            client.indices.delete(index=INDEX_NAME)
        except NotFoundError:
            logger.debug("Index not found, nothing to delete")

        client.indices.create(
            index=INDEX_NAME,
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

        # Bulk upload the records
        logger.info(f"Uploading to ES {len(features)} records...")
        bulk(client, get_actions(features))
    else:
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
                    INDEX_NAME, body=update_query, wait_for_completion=False
                )
                logger.info("Update query sent without waiting for completion")
            else:
                logger.info("No buildings to update")
