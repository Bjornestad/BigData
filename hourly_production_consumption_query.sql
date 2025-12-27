-- TOTAL ENERGY PRODUCTION AND CONSUMPTION BY DK AREA PER HOUR
-- This query combines production data (aggregated from municipalities) with consumption data
-- Shows production vs consumption balance for DK1 and DK2

SELECT
  p.year,
  p.month,
  p.day,
  p.hour,
  m.dk_area,

  -- Production breakdown by source
  ROUND(SUM(COALESCE(p.SolarMWh, 0)), 2) as solar_mwh,
  ROUND(SUM(COALESCE(p.OnshoreWindMWh, 0)), 2) as onshore_wind_mwh,
  ROUND(SUM(COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
            COALESCE(p.OffshoreWindGe100MW_MWh, 0)), 2) as offshore_wind_mwh,
  ROUND(SUM(COALESCE(p.ThermalPowerMWh, 0)), 2) as thermal_mwh,

  -- Total production
  ROUND(SUM(COALESCE(p.SolarMWh, 0) +
            COALESCE(p.OffshoreWindLt100MW_MWh, 0) +
            COALESCE(p.OffshoreWindGe100MW_MWh, 0) +
            COALESCE(p.OnshoreWindMWh, 0) +
            COALESCE(p.ThermalPowerMWh, 0)), 2) as total_production_mwh

FROM energy_production p
JOIN municipality_metadata m ON p.MunicipalityNo = m.municipality_code
WHERE p.year = 2025 AND p.month = 11  -- Change this to your desired date range
GROUP BY p.year, p.month, p.day, p.hour, m.dk_area
ORDER BY p.year DESC, p.month DESC, p.day DESC, p.hour DESC, m.dk_area;

-- NOTE: Consumption data is available in consumption_area_hourly table
-- To add consumption, you would need to query it separately due to partitioning:
--
-- SELECT year, month, day, hour, dk_area,
--        ROUND(consumption_mwh_area, 2) as consumption_mwh
-- FROM consumption_area_hourly
-- WHERE year = 2025 AND month = 11
-- ORDER BY year DESC, month DESC, day DESC, hour DESC, dk_area;
