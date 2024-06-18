# TEMPLATE FOR A FIBOA CONVERTER
#
# Copy this file and rename it to something sensible.
# The name of the file will be the name of the converter in the cli.
# If you name it 'de_abc' you'll be able to run `fiboa convert de_abc` in the cli.

from ..convert_utils import convert as convert_

# File to read the data from
# Can read any tabular data format that GeoPandas can read through read_file()
# Supported protcols: HTTP(S), GCS, S3, or the local file system

# Local URI added to the repository for initial conversion, Original Source https://beta.source.coop/esa/fusion-competition/
URI = "/home/byteboogie/work/labwork_hkerner/fieldscapes/austria/boundaries_austria_2021.gpkg"

# Unique identifier for the collection
ID = "fieldscapes_austria_2021"
# Title of the collection
TITLE = "Field boundaries for Austria (Fieldscapes)"
# Description of the collection. Can be multiline and include CommonMark.
DESCRIPTION = """ The dataset contains field boundaries for the Austria."""
# Bounding box of the data in WGS84 coordinates
BBOX = [13.239974981742014, 48.204179578647796, 16.960943738443856, 48.974515524098045]

# Provider name, can be None if not applicable, must be provided if PROVIDER_URL is provided
PROVIDER_NAME = "Euro Crops"
# URL to the homepage of the data or the provider, can be None if not applicable
PROVIDER_URL = "https://data.europa.eu/data/datasets/ama_invekosreferenzensterreich2021?locale=en"
# Attribution, can be None if not applicable
ATTRIBUTION = "Publications Office of the European Union."

# License of the data, either
# 1. a SPDX license identifier (including "dl-de/by-2-0" / "dl-de/zero-2-0"), or
LICENSE = "CC-BY-4.0"
# 2. a STAC Link Object with relation type "license"
# LICENSE = {"title": "CC-BY-4.0", "href": "https://creativecommons.org/licenses/by/4.0/", "type": "text/html", "rel": "license"}

# Map original column names to fiboa property names
# You also need to list any column that you may have added in the MIGRATION function (see below).
COLUMNS = {
    "FS_KENNUNG": "id",
    "SL_FLAECHE": "area",
    "EC_hcat_c": "crop_id",
    "EC_hcat_n": "crop_name",
    "geometry": "geometry"
}

# Add columns with constant values.
# The key is the column name, the value is a constant value that's used for all rows.
ADD_COLUMNS = {
    "determination_datetime": "2021-01-01T00:00:00Z"
}

# A list of implemented extension identifiers
EXTENSIONS = []

# Functions to migrate data in columns to match the fiboa specification.
# Example: You have a column area_m in square meters and want to convert
# to hectares as required for the area field in fiboa.
# Function signature:
#   func(column: pd.Series) -> pd.Series
COLUMN_MIGRATIONS = {
    
}

# Filter columns to only include the ones that are relevant for the collection,
# e.g. only rows that contain the word "agriculture" but not "forest" in the column "land_cover_type".
# Lamda function accepts a Pandas Series and returns a Series or a Tuple with a Series and True to inverse the mask.
COLUMN_FILTERS = {
    
}

# Custom function to migrate the GeoDataFrame if the other options are not sufficient
# This should be the last resort!
# Function signature:
#   func(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame
MIGRATION = None

# Schemas for the fields that are not defined in fiboa
# Keys must be the values from the COLUMNS dict, not the keys
MISSING_SCHEMAS = {
    "required": [ "crop_id", "crop_name" ], # i.e. non-nullable properties
    "properties": {
        "crop_id": {
            "type": "int64"
        },
        "crop_name": {
            "type": "string"
        }
    }
}


# Conversion function, usually no changes required
def convert(output_file, cache_file = None, source_coop_url = None, collection = False, compression = None):
    """
    Converts the field boundary datasets to fiboa.

    For reference, this is the order in which the conversion steps are applied:
    0. Read GeoDataFrame from file
    1. Run global migration (if provided through MIGRATION)
    2. Run filters to remove rows that shall not be in the final data
       (if provided through COLUMN_FILTERS)
    3. Add columns with constant values
    4. Run column migrations (if provided through COLUMN_MIGRATIONS)
    5. Duplicate columns (if an array is provided as the value in COLUMNS)
    6. Rename columns (as provided in COLUMNS)
    7. Remove columns (if column is not present as value in COLUMNS)
    8. Create the collection
    9. Change data types of the columns based on the provided schemas
    (fiboa spec, extensions, and MISSING_SCHEMAS)
    10. Write the data to the Parquet file

    Parameters:
    output_file (str): Path where the Parquet file shall be stored.
    cache_file (str): Path to a cached file of the data. Default: None.
                      Can be used to avoid repetitive downloads from the original data source.
    source_coop_url (str): URL to the (future) Source Cooperative repository. Default: None
    collection (bool): Additionally, store the collection separate from Parquet file. Default: False
    compression (str): Compression method for the Parquet file. Default: zstd
    kwargs: Additional keyword arguments for GeoPanda's read_file() or read_parquet() function.
    """
    convert_(
        output_file,
        cache_file,
        URI,
        COLUMNS,
        ID,
        TITLE,
        DESCRIPTION,
        BBOX,
        provider_name=PROVIDER_NAME,
        provider_url=PROVIDER_URL,
        source_coop_url=source_coop_url,
        extensions=EXTENSIONS,
        missing_schemas=MISSING_SCHEMAS,
        column_additions=ADD_COLUMNS,
        column_migrations=COLUMN_MIGRATIONS,
        column_filters=COLUMN_FILTERS,
        migration=MIGRATION,
        attribution=ATTRIBUTION,
        store_collection=collection,
        license=LICENSE,
        compression=compression,
    )