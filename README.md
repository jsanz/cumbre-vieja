# Cumbre Vieja data publishing

Data workflow to publish geospatial information related to La Palma Cumbre Vieja volcano eruption.

How to run the data script:

1. Create a Python virtual environment
2. Define the environment variables: `ES_CLOUD_ID`, `ES_USER`, and `ES_PASSWORD` or store them in an `.env` file
3. Run `python src/app.py`

Check the `app.py` script for boolean variables to control which data to process and if you want to export the datasets into the `/tmp` folder as GeoJSON files.

HTTP requests are cached in a SQLite database stored in the user cache directory (`$USER/.cache` in Linux systems).
