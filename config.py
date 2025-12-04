# Configuration file for DMI weather data fetching

# Station names mapping
STATION_NAMES = {
    "06019": "Silstrup",
    "06030": "Flyvestation Ålborg",
    "06031": "Tylstrup",
    "06032": "Stenhøj",
    "06034": "Sindal Lufthavn",
    "06041": "Skagen Fyr",
    "06049": "Hald Vest",
    "06051": "Vestervig",
    "06052": "Thyborøn",
    "06056": "Mejrup",
    "06058": "Hvide Sande",
    "06060": "Flyvestation Karup",
    "06065": "Års Syd",
    "06068": "Isenvad",
    "06069": "Foulum",
    "06070": "Århus Lufthavn",
    "06072": "Ødum",
    "06073": "Sletterhage Fyr",
    "06074": "Århus Syd",
    "06079": "Anholt Havn",
    "06080": "Esbjerg Lufthavn",
    "06081": "Blåvandshuk Fyr",
    "06082": "Borris",
    "06088": "Nordby",
    "06093": "Vester Vedsted",
    "06096": "Rømø/Juvre",
    "06102": "Bygholm Landbrugsskole",
    "06104": "Billund Lufthavn",
    "06108": "Kolding Lufthavn",
    "06109": "Askov",
    "06110": "Flyvestation Skrydstrup",
    "06116": "Store Jyndevad",
    "06118": "Sønderborg Lufthavn",
    "06119": "Kegnæs Fyr",
    "06120": "Odense Lufthavn",
    "06123": "Ørnebjerg",
    "06124": "Beldringe",
    "06126": "Kirstinebjerg",
    "06132": "Sydfyns Flyveplads",
    "06135": "Hammer Odde Fyr",
    "06138": "Christiansø",
    "06141": "Rønne Lufthavn",
    "06149": "Gedser",
    "06151": "Omø Fyr",
    "06156": "Holbæk Flyveplads",
    "06159": "Røsnæs Fyr",
    "06168": "Asnæs",
    "06169": "Nakkehoved Fyr",
    "06170": "Nordhavn",
    "06180": "København Lufthavn",
    "06181": "Jægersborg",
    "06183": "Drogden Fyr",
    "06186": "Risø",
    "06187": "Landbohøjskolen",
    "06188": "Kl. Valby",
    "06190": "Stevns Fyr",
    "06193": "Vordingborg",
}

# All available parameters from DMI
ALL_PARAMETERS = [
    # Temperature measurements
    "temp_dry", "temp_dew", "temp_mean_past1h", "temp_max_past1h", "temp_min_past1h",
    "temp_max_past12h", "temp_min_past12h", "temp_grass", "temp_grass_max_past1h",
    "temp_grass_mean_past1h", "temp_grass_min_past1h", "temp_soil", "temp_soil_max_past1h",
    "temp_soil_mean_past1h", "temp_soil_min_past1h",

    # Humidity
    "humidity", "humidity_past1h",

    # Pressure
    "pressure", "pressure_at_sea",

    # Wind
    "wind_dir", "wind_dir_past1h", "wind_speed", "wind_speed_past1h",
    "wind_gust_always_past1h", "wind_max", "wind_min_past1h", "wind_min",
    "wind_max_per10min_past1h",

    # Precipitation
    "precip_past1h", "precip_past10min", "precip_past1min", "precip_past24h",
    "precip_dur_past10min", "precip_dur_past1h",

    # Snow
    "snow_depth_man", "snow_cover_man",

    # Visibility and clouds
    "visibility", "visib_mean_last10min", "cloud_cover", "cloud_height",

    # Weather
    "weather",

    # Radiation and sun
    "radia_glob", "radia_glob_past1h", "sun_last10min_glob", "sun_last1h_glob",

    # Leaf humidity
    "leav_hum_dur_past10min", "leav_hum_dur_past1h",
]

# Active Danish synop stations (most comprehensive data)
SYNOP_STATIONS = [
    "06019",  # Silstrup
    "06030",  # Flyvestation Ålborg
    "06031",  # Tylstrup
    "06032",  # Stenhøj
    "06034",  # Sindal Lufthavn
    "06041",  # Skagen Fyr
    "06049",  # Hald Vest
    "06051",  # Vestervig
    "06052",  # Thyborøn
    "06056",  # Mejrup
    "06058",  # Hvide Sande
    "06060",  # Flyvestation Karup
    "06065",  # Års Syd
    "06068",  # Isenvad
    "06069",  # Foulum
    "06070",  # Århus Lufthavn
    "06072",  # Ødum
    "06073",  # Sletterhage Fyr
    "06074",  # Århus Syd
    "06079",  # Anholt Havn
    "06080",  # Esbjerg Lufthavn
    "06081",  # Blåvandshuk Fyr
    "06082",  # Borris
    "06088",  # Nordby
    "06093",  # Vester Vedsted (inactive as of 2024-06-27, but keeping for historical)
    "06096",  # Rømø/Juvre
    "06102",  # Bygholm Landbrugsskole
    "06104",  # Billund Lufthavn
    "06108",  # Kolding Lufthavn
    "06109",  # Askov
    "06110",  # Flyvestation Skrydstrup
    "06116",  # Store Jyndevad
    "06118",  # Sønderborg Lufthavn
    "06119",  # Kegnæs Fyr
    "06120",  # Odense Lufthavn
    "06123",  #Ørnebjerg
    "06124",  # Beldringe
    "06126",  # Kirstinebjerg
    "06132",  # Sydfyns Flyveplads
    "06135",  # Hammer Odde Fyr
    "06138",  # Christiansø
    "06141",  # Rønne Lufthavn
    "06149",  # Gedser
    "06151",  # Omø Fyr
    "06156",  # Holbæk Flyveplads
    "06159",  # Røsnæs Fyr
    "06168",  # Asnæs
    "06169",  # Nakkehoved Fyr
    "06170",  # Nordhavn
    "06180",  # København Lufthavn
    "06181",  # Jægersborg
    "06183",  # Drogden Fyr
    "06186",  # Risø
    "06187",  # Landbohøjskolen
    "06188",  # Kl. Valby
    "06190",  # Stevns Fyr
    "06193",  # Vordingborg
]

# Default parameters for testing (most common and reliable)
DEFAULT_PARAMETERS = [
    "temp_dry",
    "humidity",
    "pressure",
    "wind_speed",
    "wind_dir",
    "precip_past1h",
]

# Test stations (smaller subset for initial testing)
TEST_STATIONS = [
    "06186",  # Risø (Copenhagen area)
    "06080",  # Esbjerg Lufthavn
    "06041",  # Skagen Fyr
]
