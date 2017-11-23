# itinerum-tripbreaker

The repository contains a basic proof-of-concept algorithm for inferencing trips from Itinerum users' collected GPS coordinates. This code within is unsupported, provided as-is, and is intended as a jumping off point for building more robust trip detection methods.

The algorithm employs a naïve rule-based approach which first over-eagerly divides all points into segments and attempts to reconstruct full trips from these segments when various conditions are met.

### Getting Started

Two helper scripts (*run_tripbreaker_on_user.py* and *run_tripbreaker_on_survey.py*) are included to run the algorithm on a single-user or for all users in a survey. The scripts assume coordinates data is to be loaded from a PostGIS SQL table, however, this can be adapted to read directly from a .csv input file. The helper scripts also read a metro.csv file if exists, which requires simply an X (longitude) and Y (latitude) for each metro stop to include in trip detection. An example is included within the repository.

The file *tripbreaker/algorithm.py* contains the tripbreaking functions (in processing order), which are called by the super function *run()* at the bottom. The tripbreaker results two objects: *trips* and *summaries*. The objects' keys are a Trip ID (not necessarily consecutive) that will be identical between both. The *trips* object values will include all the GPS coordinates labelled by trip, the *summaries* object values will include a single record indicating the start/end locations & times of each detected trip.

### Configuration

The helper script *run_tripbreaker_on_user.py* contains the complete configuration object with the following parameters:

| Parameter              | Sub-Parameter  | Value / Type                             |
| ---------------------- | -------------- | ---------------------------------------- |
| in_db_uri              |                | SQL database connection URI (string)     |
| out_db_uri             |                | SQL database connection URI (string)     |
| survey_name            |                | Survey name (string)                     |
| mobile_user_email      |                | Email address (string)                   |
| start                  |                | Coordinate period start (datetime)       |
| end                    |                | Coordinate period end (datetime)         |
| tripbreaker_parameters | break_interval | Minimum seconds required as a break for stop to be detected (integer) |
| tripbreaker_parameters | subway_buffer  | Meters buffer to extend from station centroid to associate with station for transfers (integer) |
| input_srid             |                | Projection SRID of input coordinates [lat/lon: 4326] (integer) |
| output_srid            |                | Projection SRID for output coordinates (integer) |

When helper scripts have been configured as desired, the tripbreaker may be run with `python run_tripbreaker_on_user.py` . Default output is to a PostGIS-enabled SQL table named `detected_trips`. 