-- SIMPLIFIED ML TRAINING DATASET
-- Weather features (INPUT) → Total Production & Total Consumption (OUTPUT)
--
-- Date Range: 2021-2025, Hourly data by DK Area (DK1/DK2)

SELECT
  -- Time identifiers
  w.year,
  w.month,
  w.day,
  w.hour,
  w.dk_area,

  -- Temporal features (useful for ML patterns)
  w.month as month_of_year,
  w.hour as hour_of_day,

  -- WEATHER FEATURES (INPUT) ----------------------------------------

  -- Wind features
  w.wind_speed_mean_area as wind_speed_avg,
  w.wind_speed_max_area as wind_speed_max,
  w.wind_dir_sin_area as wind_direction_sin,
  w.wind_dir_cos_area as wind_direction_cos,

  -- Solar features
  w.radia_glob_past1h_area as solar_radiation_1h,
  w.sun_last1h_glob_area as sunshine_duration_1h,
  w.cloud_cover_mean_area as cloud_cover_avg,

  -- Data quality (number of weather stations used)
  w.n_stations_wind,
  w.n_stations_solar,

  -- OUTPUT VARIABLES (TARGET) ---------------------------------------

  -- Total energy production (MWh)
  p.total_production_mwh,

  -- Total energy consumption (MWh)
  c.total_consumption_mwh

FROM weather_wind_solar_area_hourly w

-- Join production data
LEFT JOIN (
  SELECT
    p.year,
    p.month,
    p.day,
    p.hour,
    m.dk_area,
    ROUND(SUM(
      COALESCE(p.SolarMWh, 0) +
      COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
      COALESCE(p.OffshoreWindGe100MW_MWh, 0) +
      COALESCE(p.OnshoreWindMWh, 0) +
      COALESCE(p.ThermalPowerMWh, 0)
    ), 2) as total_production_mwh
  FROM energy_production p
  JOIN municipality_metadata m ON p.MunicipalityNo = m.municipality_code
  WHERE p.year >= 2021  -- Adjust date range as needed
  GROUP BY p.year, p.month, p.day, p.hour, m.dk_area
) p
  ON w.dk_area = p.dk_area
  AND w.year = p.year
  AND w.month = p.month
  AND w.day = p.day
  AND w.hour = p.hour

-- Join consumption data
LEFT JOIN (
  SELECT
    year,
    month,
    day,
    hour,
    PriceArea as dk_area,
    ROUND(SUM(ShareMWh), 2) as total_consumption_mwh
  FROM consumption_coverage_location
  WHERE year >= 2021  -- Adjust date range as needed
  GROUP BY year, month, day, hour, PriceArea
) c
  ON w.dk_area = c.dk_area
  AND w.year = c.year
  AND w.month = c.month
  AND w.day = c.day
  AND w.hour = c.hour

WHERE w.year >= 2021  -- Adjust as needed
  AND p.total_production_mwh IS NOT NULL  -- Only include rows with production data
  AND c.total_consumption_mwh IS NOT NULL  -- Only include rows with consumption data

ORDER BY w.year DESC, w.month DESC, w.day DESC, w.hour DESC, w.dk_area;

--
-- FEATURES (12 columns):
-- - wind_speed_avg, wind_speed_max
-- - wind_direction_sin, wind_direction_cos
-- - solar_radiation_1h, sunshine_duration_1h, cloud_cover_avg
-- - month_of_year, hour_of_day
-- - n_stations_wind, n_stations_solar
-- - dk_area
--
-- TARGETS (2 columns):
-- - total_production_mwh
-- - total_consumption_mwh
--
