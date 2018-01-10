# itinerum-tripbreaker

_The repository contains a basic proof-of-concept algorithm for inferencing trips from Itinerum users' collected GPS coordinates. This code within is **unsupported**, provided **as-is**, and is intended as a jumping off point for building more robust trip detection methods._

_The algorithm employs a naïve rule-based approach which first over-eagerly divides all points into segments and attempts to reconstruct full trips from these segments when various conditions are met._

### Getting Started

The repository is structured as:

 - `./data`: directory for all input and generated data such as the Itinerum platform exported .csvs
 - `./run_tripbreaker`: contains all Python tripbreaker code
- `./shapefiles`: any shapefiles used as base maps to display Itinerum points and trip breaker results in QGIS
- `load_csvs_to_sqlite.py`: script to load Itinerum .csv exports to SQLite database for use by tripbreaker algorithm
- `subway_geojson_to_csv.py`: converts OSM geojson export to .csv for use by tripbreaker algorithm
- `visualize_results.qgs`: a QGIS save file which can be used to study trip breaker results



1. A PostgreSQL database with PostGIS enabled must be available to write tripbreaker results to.

2. Load all Itinerum data in the `./data` directory. This directory should include the Itinerum platform's .csv exports and any available subway stations data.

3. If the subway stations data is supplied as an OSM generated .geojson, edit the `subway_geojson_to_csv.py` file with the correct filenames and run the script to generate a .csv version

   *For example:*

   ​	``` $ python subway_geojson_to_csv.py```

4. Edit `load_csvs_to_sqlite.py` with the correct survey name variable and run. Ensure the `table_map` values for the supplied .csv names match the format of the input .csv filenames.

5. Edit either `./run_tripbreaker/run_tripbreaker_on_user.py` or `./run_tripbreaker/run_tripbreaker_on_survey.py` with the correct configuration options (*described below*). Run the helper scripts to generate tripbreaker results for either a single user or across the whole survey respectively. *This will reset the database tables on every run unless edited out.*

*Notes:*

Optionally, subway stations data can be provided for trip detection which requires an X (longitude) and Y (latitude) for each metro stop. An example is included within the repository.

The file *tripbreaker/algorithm.py* contains the tripbreaking functions (in processing order), which are called by the *run()* function at the bottom of the script. The tripbreaker results in two objects: *trips* and *summaries*. The objects' keys are a Trip ID (not necessarily consecutive) that will be identical between both. The *trips* object values will include all the GPS coordinates labelled by trip, the *summaries* object values will include a single record indicating the start/end locations & times of each detected trip.

### Configuration

The helper script *run_tripbreaker_on_user.py* contains the complete configuration object with the following parameters:

| Parameter              | Sub-Parameter          | Value / Type                             |
| ---------------------- | ---------------------- | ---------------------------------------- |
| in_db_uri              |                        | SQL database connection URI (string)     |
| out_db_uri             |                        | PostGIS-enabled database connection URI (string) |
| mobile_uuid            |                        | The mobile user's uuid to run trip breaker on [only used by run_tripbreak_on_user.py] (string) |
| tripbreaker_parameters | break_interval         | Minimum seconds required as a break for stop to be detected (integer) |
| tripbreaker_parameters | subway_buffer          | Meters buffer to extend from station centroid to associate with station for transfers (integer) |
| tripbreaker_parameters | accuracy_cutoff_meters | Ignore all points with an accuracy reading of the specified precision or better (integer) |
| subway_stations_csv    |                        | Full relative filepath for the subway stations .csv data |
| input_srid             |                        | Projection SRID of input coordinates [lat/lon: 4326] (integer) |
| output_srid            |                        | Projection SRID for output coordinates (integer) |

When helper scripts have been configured as desired, the tripbreaker may be run with `python run_tripbreaker_on_user.py`  or `python run_tripbreaker_on_survey.py`.

### Viewing Results

A demo .qgs save file and Toronto base data is included to quickly view tripbreaker results. Subsets of the data can be individually queried and loaded on-demand using the DB Manager within QGIS. 