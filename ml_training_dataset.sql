-- ML TRAINING DATASET: WEATHER → ENERGY PRODUCTION & CONSUMPTION
--
-- This query creates a complete dataset for machine learning with:
-- - Weather features (temperature, wind, solar radiation, etc.) as INPUT
-- - Energy production and consumption as OUTPUT/TARGET variables
--
-- Date Range: 2021-2025, Hourly granularity, By DK Area (DK1/DK2)

WITH weather_features AS (
  SELECT
    dk_area,
    year,
    month,
    day,
    hour,
    -- Wind features (for predicting wind energy production)
    wind_speed_mean_area as wind_speed_avg,
    wind_speed_max_area as wind_speed_max,
    wind_dir_sin_area as wind_direction_sin,
    wind_dir_cos_area as wind_direction_cos,
    -- Solar features (for predicting solar energy production)
    radia_glob_past1h_area as solar_radiation_1h,
    sun_last1h_glob_area as sunshine_duration_1h,
    cloud_cover_mean_area as cloud_cover_avg,
    -- Data quality
    n_stations_wind,
    n_stations_solar
  FROM weather_wind_solar_area_hourly
  WHERE year >= 2021  -- Adjust date range as needed
),
production_data AS (
  SELECT
    p.year,
    p.month,
    p.day,
    p.hour,
    m.dk_area,
    -- Production by energy source
    ROUND(SUM(COALESCE(p.SolarMWh, 0)), 2) as solar_production_mwh,
    ROUND(SUM(COALESCE(p.OnshoreWindMWh, 0)), 2) as onshore_wind_production_mwh,
    ROUND(SUM(COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
              COALESCE(p.OffshoreWindGe100MW_MWh, 0)), 2) as offshore_wind_production_mwh,
    ROUND(SUM(COALESCE(p.ThermalPowerMWh, 0)), 2) as thermal_production_mwh,
    -- Total wind production (for wind-based ML models)
    ROUND(SUM(COALESCE(p.OnshoreWindMWh, 0) +
              COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
              COALESCE(p.OffshoreWindGe100MW_MWh, 0)), 2) as total_wind_production_mwh,
    -- Total production
    ROUND(SUM(COALESCE(p.SolarMWh, 0) +
              COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
              COALESCE(p.OffshoreWindGe100MW_MWh, 0) +
              COALESCE(p.OnshoreWindMWh, 0) +
              COALESCE(p.ThermalPowerMWh, 0)), 2) as total_production_mwh
  FROM energy_production p
  JOIN municipality_metadata m ON p.MunicipalityNo = m.municipality_code
  WHERE p.year >= 2021  -- Adjust date range as needed
  GROUP BY p.year, p.month, p.day, p.hour, m.dk_area
),
consumption_data AS (
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
)

-- FINAL DATASET: Join all features and targets
SELECT
  -- Time features
  w.year,
  w.month,
  w.day,
  w.hour,
  w.dk_area,

  -- Temporal features (useful for ML)
  w.month as month_of_year,
  w.hour as hour_of_day,
  -- Day of week (Monday=1, Sunday=7)
  CASE
    WHEN pmod(datediff(concat(w.year, '-', lpad(w.month, 2, '0'), '-', lpad(w.day, 2, '0')), '1970-01-05'), 7) = 0 THEN 7
    ELSE pmod(datediff(concat(w.year, '-', lpad(w.month, 2, '0'), '-', lpad(w.day, 2, '0')), '1970-01-05'), 7)
  END as day_of_week,
  -- Is weekend (1=yes, 0=no)
  CASE
    WHEN pmod(datediff(concat(w.year, '-', lpad(w.month, 2, '0'), '-', lpad(w.day, 2, '0')), '1970-01-05'), 7) IN (5, 6) THEN 1
    ELSE 0
  END as is_weekend,

  -- WEATHER FEATURES (INPUT)
  w.wind_speed_avg,
  w.wind_speed_max,
  w.wind_direction_sin,
  w.wind_direction_cos,
  w.solar_radiation_1h,
  w.sunshine_duration_1h,
  w.cloud_cover_avg,
  w.n_stations_wind,
  w.n_stations_solar,

  -- ENERGY PRODUCTION TARGETS (OUTPUT)
  p.solar_production_mwh,
  p.onshore_wind_production_mwh,
  p.offshore_wind_production_mwh,
  p.total_wind_production_mwh,
  p.thermal_production_mwh,
  p.total_production_mwh,

  -- ENERGY CONSUMPTION TARGET (OUTPUT)
  c.total_consumption_mwh,

  -- DERIVED FEATURES
  ROUND(p.total_production_mwh - c.total_consumption_mwh, 2) as net_energy_balance_mwh,
  ROUND(100.0 * p.total_production_mwh / NULLIF(c.total_consumption_mwh, 0), 2) as self_sufficiency_pct

FROM weather_features w
LEFT JOIN production_data p
  ON w.dk_area = p.dk_area
  AND w.year = p.year
  AND w.month = p.month
  AND w.day = p.day
  AND w.hour = p.hour
LEFT JOIN consumption_data c
  ON w.dk_area = c.dk_area
  AND w.year = c.year
  AND w.month = c.month
  AND w.day = c.day
  AND w.hour = c.hour

WHERE w.year >= 2021  -- Adjust as needed
  AND p.total_production_mwh IS NOT NULL  -- Exclude rows with missing production data
  AND c.total_consumption_mwh IS NOT NULL  -- Exclude rows with missing consumption data

ORDER BY w.year DESC, w.month DESC, w.day DESC, w.hour DESC, w.dk_area;
