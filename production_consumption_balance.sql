-- HOURLY ENERGY PRODUCTION AND CONSUMPTION BY DK AREA (DK1/DK2)
-- Shows total production, consumption, and net balance per hour

WITH production AS (
  SELECT
    p.year,
    p.month,
    p.day,
    p.hour,
    m.dk_area,
    ROUND(SUM(COALESCE(p.SolarMWh, 0) +
              COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
              COALESCE(p.OffshoreWindGe100MW_MWh, 0) +
              COALESCE(p.OnshoreWindMWh, 0) +
              COALESCE(p.ThermalPowerMWh, 0)), 2) as total_production_mwh
  FROM energy_production p
  JOIN municipality_metadata m ON p.MunicipalityNo = m.municipality_code
  WHERE p.year = 2025 AND p.month = 11  -- Change date range as needed
  GROUP BY p.year, p.month, p.day, p.hour, m.dk_area
),
consumption AS (
  SELECT
    year,
    month,
    day,
    hour,
    PriceArea as dk_area,
    ROUND(SUM(ShareMWh), 2) as total_consumption_mwh
  FROM consumption_coverage_location
  WHERE year = 2025 AND month = 11  -- Change date range as needed
  GROUP BY year, month, day, hour, PriceArea
)
SELECT
  p.year,
  p.month,
  p.day,
  p.hour,
  p.dk_area,
  p.total_production_mwh,
  c.total_consumption_mwh,
  ROUND(p.total_production_mwh - c.total_consumption_mwh, 2) as net_balance_mwh,
  ROUND(100.0 * p.total_production_mwh / c.total_consumption_mwh, 2) as self_sufficiency_pct
FROM production p
LEFT JOIN consumption c
  ON p.year = c.year
  AND p.month = c.month
  AND p.day = c.day
  AND p.hour = c.hour
  AND p.dk_area = c.dk_area
ORDER BY p.year DESC, p.month DESC, p.day DESC, p.hour DESC, p.dk_area;
