DATA_TYPES = ["generation", "weather"] 

normalized_columns = {
    "generation": {
        "time": "time",
        "columns": 
            [   
                "total_load_actual",
                "price_day_ahead",
                "price_actual",
                "generation_fossil_hard_coal",
                "generation_fossil_gas",
                "generation_fossil_brown_coal_lignite",
                "generation_fossil_oil",
                "generation_other_renewable",
                "generation_waste",
                "generation_biomass",
                "generation_other",
                "generation_solar",
                "generation_hydro_water_reservoir",
                "generation_nuclear",
                "generation_hydro_run_of_river_and_poundage",
                "generation_wind_onshore",
                "generation_hydro_pumped_storage_consumption"

            ]
        },
    "weather": {
        "time": "dt_iso",
        "columns": 
            [
                "city_name",
                "temp",
                "pressure",
                "humidity",
                "wind_speed",
                "wind_deg",
                "rain_1h",
                "rain_3h",
                "snow_3h",
                "clouds_all",
            ]
        }
    }