import logging
from datetime import datetime
from copy import deepcopy

import requests_cache

from elasticsearch import NotFoundError
from elasticsearch.exceptions import RequestError

from geojson_rewind import rewind
from area import area
from shapely.geometry import shape, mapping
from shapely.geometry.multipolygon import MultiPolygon
from shapely.validation import make_valid

# from shapely.validation import make_valid

from data import IDS, LOC_CANARY


logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")

session = requests_cache.CachedSession("http_cache", use_cache_dir=True)

INDEX_NAME = "lapalma"
GEOJSON_URL = "https://opendata.arcgis.com/api/v3/datasets/{id}/downloads/data?"
GEOJSON_PARAMS = {"format": "geojson", "spatialRefId": "4326"}


def create_footprints_index(client):
    """
    Creates the index to host the footprints data
    """
    # Create the index if absent
    if not client.indices.exists(index=INDEX_NAME):
        client.indices.create(
            index=INDEX_NAME,
            settings={"number_of_shards": 1, "number_of_replicas": 1},
            mappings={
                "properties": {
                    "id": {"type": "text"},
                    "timestamp": {"type": "date"},
                    "geometry": {"type": "geo_shape"},
                    "area": {"type": "long"},
                    "diff_id": {"type": "text"},
                    "diff_timestamp": {"type": "date"},
                    "diff_geometry": {"type": "geo_shape"},
                    "diff_area": {"type": "long"},
                }
            },
        )


def download_footprints():
    """
    Downloads the footprints from La Palma data portal and returns
    a list of dictionaries with the geometries with their identifier,
    timestamp and area.
    """

    # Process the features
    features = []

    # Get the IDs into the features and sort them
    for id_date in IDS:
        id = id_date[0]

        # Download the GeoJSON and store the fixed geometry
        logger.debug(f"Getting the resource [{id}] ...")
        url = GEOJSON_URL.format(id=id)
        r = session.get(url, params=GEOJSON_PARAMS)

        if r.status_code != 200:
            logger.error(f"Resource [{id}] not found at {url}")
        else:
            json_dataset = r.json()
            if "features" in json_dataset and "geometry" in json_dataset["features"][0]:
                geometry = rewind(json_dataset["features"][0]["geometry"])
                geom_area = int(area(geometry))
                timestamp = datetime.strptime(
                    f"{id_date[1]} {id_date[2]}", "%Y-%m-%d %H:%M"
                )

                features.append(
                    {
                        "id": id,
                        "geometry": geometry,
                        "timestamp": LOC_CANARY.localize(timestamp).isoformat(),
                        "area": geom_area,
                    }
                )
    return features


def filter_area(polygon):
    TOLERANCE = 1
    return area(mapping(polygon)) > TOLERANCE


def get_diffed_features(features):
    """
    Extends the footprints with the difference with the previous footprint
    """
    TOLERANCE = 0.000001
    sorted_features = sorted(features, key=lambda f: f["timestamp"])
    diffed_features = []

    for idx, f in enumerate(sorted_features):
        curr_feature = deepcopy(f)
        prev_feature = sorted_features[idx - 1] if idx > 0 else None

        curr_geom = shape(curr_feature["geometry"])

        if prev_feature is None:
            logger.debug(f"{curr_feature['id']} has no previous feature")
            diff_geom = curr_geom
        else:
            diff_geom = curr_geom.difference(shape(prev_feature["geometry"])).simplify(
                TOLERANCE, preserve_topology=True
            )
            if diff_geom.geom_type == 'MultiPolygon':
                # Remove small polygons
                parts = len(diff_geom.geoms)
                diff_geom = MultiPolygon([poly for poly in diff_geom.geoms if filter_area(poly)])
                parts_after = len(diff_geom.geoms)
                if parts != parts_after:
                    logger.debug(f"{parts - parts_after} small parts removed")

        # Try to fix any invalid geometries
        if not diff_geom.is_valid:
            diff_geom = make_valid(diff_geom)
        if not diff_geom.is_valid:
            logger.debug("Trying to fix the geometry with the buffer trick")
            diff_geom = diff_geom.buffer(0.0)

        if diff_geom.is_valid:
            diff_geom_geojson = mapping(diff_geom)

            # Create the new properties
            diff_feature = {
                "diff_id": prev_feature["id"] if prev_feature else None,
                "diff_timestamp": prev_feature["timestamp"] if prev_feature else None,
                "diff_geometry": diff_geom_geojson,
                "diff_area": int(area(diff_geom_geojson)),
            }
            diff_feature.update(curr_feature)

            diffed_features.append(diff_feature)
        else:
            logger.warning(f"SKIPPING [{id}], check the geometry")

    return diffed_features


def upload_footrpint(client, doc):
    id = doc["id"]
    try:
        logger.debug(f"[{id}] uploading to ES...")
        client.index(index=INDEX_NAME, id=id, document=doc)
        return True
    except RequestError as e:
        logger.error(f"Error uploading [{id}] with: {e.error}")
        logger.debug(f"{e.info}")
        return False


def index_footprints(client, features, overwrite=False):
    """
    Uploads to Elasticsearch the features not found in the index
    """
    results = {"indexed": 0, "errors": 0, "skipped": 0}
    for doc in features:
        id = doc["id"]
        upload = overwrite
        if not overwrite:
            try:
                client.get(index=INDEX_NAME, id=id)
                print(f"[{id}] found in ES...")
                upload = False
            except NotFoundError:
                upload = True

        if upload:
            uploaded = upload_footrpint(client, doc)
            if uploaded:
                results["indexed"] = results["indexed"] + 1
            else:
                results["errors"] = results["errors"] + 1
        else:
            results["skipped"] = results["skipped"] + 1
    return results
