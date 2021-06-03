year = 2021

bouy_data_loc = {
    'base_site': 'https://www.ndbc.noaa.gov/',
    'standard_meterological_data': 'stdmet',
    'spectral_wave_density_data': 'swden',
    'spectral_wave_alpha1_direction_data': 'swdir',
    'spectral_wave_alpha2_direction_data': 'swdir2',
    'spectral_wave_r1_direction_data':'swr1',
    'spectral_wave_r2_direction_data': 'swr2',
    'supplemental_measurement_data': 'supl'
}

def buoy_data_months():
    i = 0
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    while True:
        yield months[i]
        i += 1
    
snow_data_site = {}

buoy_data = {
    'cols': {
        'STATION_ID': 'TEXT',
        'POINT_ID': 'TEXT PRIMARY_KEY',
        'YEAR': 'INTEGER',
        'MONTH': 'INTEGER',
        'DAY': 'INTEGER',
        'HOUR': 'INTEGER',
        'MIN': 'INTEGER',
        'DATE': 'TEXT',
        'WDIR': 'REAL',
        'WSPD': 'REAL',
        'GST': 'REAL',
        'WVHT': 'REAL',
        'DPD': 'REAL',
        'APD': 'REAL',
        'MWD': 'REAL',
        'PRES': 'REAL',
        'ATMP': 'REAL',
        'WTMP': 'REAL',
        'DEWP': 'REAL',
        'VIS': 'REAL',
        'TIDE': 'REAL'
    },
    'supplemental_info':
        """
        FOREIGN KEY (STATION_ID)
        REFERENCES buoys (STATION)
            ON UPDATE CASCADE
            ON UPDATE CASCADE
        """
}

buoys = {
    'cols': {
        'STATION_ID': 'TEXT PRIMARY KEY',
        'latitude': 'TEXT',
        'longitude': 'TEXT',
        'name': 'TEXT',
        'owner': 'TEXT',
        'pgm': 'TEXT',
        'type': 'TEXT',
        'met': 'TEXT',
        'currents': 'TEXT',
        'waterquality': 'TEXT',
        'dart': 'TEXT'
    }
}


snow_data = {
    'cols': {
        'date': 'TEXT',
        'snowfall': 'TEXT'
    }
}

snow_stations = {
    'cols': {
        'ghcnid': 'TEXT',
        'station_name': 'TEXT',
        'state': 'TEXT',
        'county': 'TEXT',
        'elev': 'TEXT'
    }
}

table_dict = {
    'buoy_data': buoy_data,
    'buoys': buoys,
    'snow_data': snow_data,
    'snow_stations': snow_stations
}