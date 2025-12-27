-- Total Energy Production and Consumption by DK Area (DK1/DK2) Per Hour
-- Combines production data (from municipalities) with consumption data

WITH hourly_production AS (
  SELECT
    p.year,
    p.month,
    p.day,
    p.hour,
    m.dk_area,
    SUM(COALESCE(p.SolarMWh, 0) +
        COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
        COALESCE(p.OffshoreWindGe100MW_MWh, 0) +
        COALESCE(p.OnshoreWindMWh, 0) +
        COALESCE(p.ThermalPowerMWh, 0)) as total_production_mwh,
    SUM(COALESCE(p.SolarMWh, 0)) as solar_mwh,
    SUM(COALESCE(p.OnshoreWindMWh, 0)) as onshore_wind_mwh,
    SUM(COALESCE(p.OffshoreWindLt100MW_MWh, 0) + COALESCE(p.OffshoreWindGe100MW_MWh, 0)) as offshore_wind_mwh,
    SUM(COALESCE(p.ThermalPowerMWh, 0)) as thermal_mwh
  FROM energy_production p
  JOIN municipality_metadata m ON p.MunicipalityNo = m.municipality_code
  GROUP BY p.year, p.month, p.day, p.hour, m.dk_area
),
hourly_consumption AS (
  SELECT
    year,
    month,
    day,
    hour,
    dk_area,
    consumption_mwh_area as total_consumption_mwh
  FROM consumption_area_hourly
)
SELECT
  COALESCE(prod.year, cons.year) as year,
  COALESCE(prod.month, cons.month) as month,
  COALESCE(prod.day, cons.day) as day,
  COALESCE(prod.hour, cons.hour) as hour,
  COALESCE(prod.dk_area, cons.dk_area) as dk_area,

  -- Production breakdown
  COALESCE(prod.total_production_mwh, 0) as total_production_mwh,
  COALESCE(prod.solar_mwh, 0) as solar_mwh,
  COALESCE(prod.onshore_wind_mwh, 0) as onshore_wind_mwh,
  COALESCE(prod.offshore_wind_mwh, 0) as offshore_wind_mwh,
  COALESCE(prod.thermal_mwh, 0) as thermal_mwh,

  -- Consumption
  COALESCE(cons.total_consumption_mwh, 0) as total_consumption_mwh,

  -- Net balance (positive = export, negative = import)
  COALESCE(prod.total_production_mwh, 0) - COALESCE(cons.total_consumption_mwh, 0) as net_balance_mwh,

  -- Self-sufficiency percentage
  ROUND(
    100.0 * COALESCE(prod.total_production_mwh, 0) /
    NULLIF(COALESCE(cons.total_consumption_mwh, 0), 0),
    2
  ) as self_sufficiency_pct

FROM hourly_production prod
FULL OUTER JOIN hourly_consumption cons
  ON prod.year = cons.year
  AND prod.month = cons.month
  AND prod.day = cons.day
  AND prod.hour = cons.hour
  AND prod.dk_area = cons.dk_area

ORDER BY year DESC, month DESC, day DESC, hour DESC, dk_area;
