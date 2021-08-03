# Snowcast Snowfall Predictor

Model to pull data from NOAA buoys in the Pacific Ocean &amp; Rocky Mountain
snow reports to predict future snowfall levels.

## Getting Started

### SQLite
Snowcast utilizes a SQLite backend for data storage. Before downloading data,
initialize a blank SQLite DB.

### Tables
Snowcast uses 4 tables to store data: `buoys`, `buoy_data`, `snow_stations`, and
 `snow_data`
* `buoys`: metadata about each buoy, including location, type, and buoy name.
* `buoy_data`: measurement data from each buoy. A description of each
column can be found in `support_files`
* `snow_stations`: metadata about each snowfall measurement station, including
location, elevation, and station name.
* `snow_data`: measurement data from each snow station. A description of each
column can be found in `support_files`

### Creating Tables
Creating each table in your database is as simple as initializing a SQLUtil
instance with the path to your database and calling the `make_table()` method
for each table.

```
from sqlite_helpers import SQLUtil

sql_util = SQLUtil('path_to_your.db')
sql_util.make_table('buoys')
```

### Populating data
Once the tables have been created, you can populate the data for each table.
`buoys` and `snow_stations` can each be populated for all buoys & snow stations
with a single call for each table. Data for a single buoy or snow station can be
downloaded as well. The only time these tables would need to be updated is if a
buoy/station is added or changed.

```
from download_data import Downloader

dler = Downloader()

# Downloads metadata for all buoys
dler.buoy_dl_metadata()

# Downloads data for a particular buoy
dler.buoy_dl_metadata('51001')
```
Measurement data for buoys and snow stations can be downloaded for individual
buoys or stations. Buoy data is stored by NOAA differently so different methods
are needed to download data for the current year vs previous years.

```
from download_data import Downloader

dler = Downloader()

# Download buoy measurement data for the current year.
dler.buoy_dl_current_year('51001')

# Download buoy measurement data for all previous years.
dler.buoy_dl_previous_years('51001')

# Download snowfall data for a given snow station
dler.snow_dl_data('USC00420072')
```
