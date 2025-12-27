# Complete List of All 44 Weather Parameters

Based on weather_observations_raw table (198,866,490 total observations)

## Parameters Sorted by Observation Count

| # | Parameter ID | Count | % of Total | Description |
|---|-------------|---------|-----------|-------------|
| 1 | temp_dry | 13,532,352 | 6.8% | **Dry bulb temperature (°C)** - Primary temperature measurement |
| 2 | humidity | 13,474,397 | 6.8% | **Relative humidity (%)** - Moisture in air |
| 3 | temp_dew | 13,473,127 | 6.8% | **Dew point temperature (°C)** - Temperature at which air becomes saturated |
| 4 | wind_dir | 12,551,167 | 6.3% | **Wind direction (degrees)** - Direction wind is coming from (0-360°) |
| 5 | wind_speed | 12,551,167 | 6.3% | **Wind speed (m/s)** - Current wind speed |
| 6 | wind_max | 12,421,871 | 6.2% | **Maximum wind speed** - Peak wind gust |
| 7 | pressure | 11,929,065 | 6.0% | **Atmospheric pressure** - Station-level pressure |
| 8 | pressure_at_sea | 11,821,260 | 5.9% | **Sea-level pressure** - Pressure adjusted to sea level |
| 9 | precip_past10min | 8,817,550 | 4.4% | **Precipitation last 10 min (mm)** - Recent rainfall |
| 10 | visib_mean_last10min | 7,070,759 | 3.6% | **Mean visibility last 10 min (m)** - Average visibility |
| 11 | visibility | 6,155,389 | 3.1% | **Current visibility (m)** - How far you can see |
| 12 | radia_glob | 5,979,663 | 3.0% | **Global radiation (W/m²)** - Solar radiation measurement |
| 13 | precip_dur_past10min | 5,891,669 | 3.0% | **Precipitation duration last 10 min** - How long it rained |
| 14 | wind_min | 5,831,831 | 2.9% | **Minimum wind speed** - Lowest wind speed |
| 15 | sun_last10min_glob | 5,079,000 | 2.6% | **Sunshine duration last 10 min** - Direct sunlight time |
| 16 | temp_grass | 5,042,040 | 2.5% | **Grass temperature (°C)** - Temperature at grass level |
| 17 | cloud_cover | 4,673,952 | 2.4% | **Cloud coverage** - Amount of sky covered by clouds |
| 18 | temp_soil | 3,511,958 | 1.8% | **Soil temperature (°C)** - Underground temperature |
| 19 | cloud_height | 3,422,504 | 1.7% | **Cloud base height (m)** - How high clouds are |
| 20 | precip_past1min | 3,159,372 | 1.6% | **Precipitation last 1 min (mm)** - Very recent rainfall |
| 21 | leav_hum_dur_past10min | 3,026,120 | 1.5% | **Leaf wetness duration last 10 min** - Agricultural sensor |
| 22 | weather | 2,667,845 | 1.3% | **Weather condition code** - Rain, snow, fog, etc. |
| 23 | humidity_past1h | 2,241,577 | 1.1% | **Humidity last hour (%)** - Hourly aggregate |
| 24 | temp_mean_past1h | 2,240,824 | 1.1% | **Mean temperature last hour (°C)** - Hourly average |
| 25 | temp_max_past1h | 2,239,988 | 1.1% | **Max temperature last hour (°C)** - Hourly peak |
| 26 | temp_min_past1h | 2,239,342 | 1.1% | **Min temperature last hour (°C)** - Hourly low |
| 27 | wind_speed_past1h | 2,085,373 | 1.0% | **Wind speed last hour (m/s)** - Hourly wind |
| 28 | wind_dir_past1h | 2,085,373 | 1.0% | **Wind direction last hour (°)** - Hourly wind direction |
| 29 | wind_gust_always_past1h | 2,083,870 | 1.0% | **Wind gust last hour** - Maximum gust in past hour |
| 30 | wind_max_per10min_past1h | 1,604,152 | 0.8% | **Max wind per 10min last hour** - Peak 10-min wind |
| 31 | precip_past1h | 1,466,815 | 0.7% | **Precipitation last hour (mm)** - Hourly rainfall total |
| 32 | wind_min_past1h | 1,058,245 | 0.5% | **Min wind last hour (m/s)** - Lowest wind in hour |
| 33 | radia_glob_past1h | 995,657 | 0.5% | **Global radiation last hour (W/m²)** - Hourly solar |
| 34 | sun_last1h_glob | 856,213 | 0.4% | **Sunshine duration last hour** - Total sun time in hour |
| 35 | precip_dur_past1h | 806,545 | 0.4% | **Precipitation duration last hour** - How long it rained |
| 36 | temp_grass_mean_past1h | 681,261 | 0.3% | **Mean grass temp last hour (°C)** - Hourly grass temp avg |
| 37 | temp_grass_min_past1h | 681,259 | 0.3% | **Min grass temp last hour (°C)** - Hourly grass temp low |
| 38 | temp_grass_max_past1h | 681,257 | 0.3% | **Max grass temp last hour (°C)** - Hourly grass temp high |
| 39 | temp_soil_mean_past1h | 585,467 | 0.3% | **Mean soil temp last hour (°C)** - Hourly soil temp avg |
| 40 | temp_soil_max_past1h | 582,749 | 0.3% | **Max soil temp last hour (°C)** - Hourly soil temp high |
| 41 | temp_soil_min_past1h | 582,735 | 0.3% | **Min soil temp last hour (°C)** - Hourly soil temp low |
| 42 | leav_hum_dur_past1h | 515,661 | 0.3% | **Leaf wetness duration last hour** - Hourly leaf wetness |
| 43 | temp_min_past12h | 234,665 | 0.1% | **Min temperature last 12 hours (°C)** - Half-day low |
| 44 | temp_max_past12h | 233,404 | 0.1% | **Max temperature last 12 hours (°C)** - Half-day high |

---

## Parameter Categories

### 🌡️ **Temperature (13 parameters)**
- **Current**: temp_dry, temp_dew, temp_grass, temp_soil
- **Hourly aggregates**: temp_mean_past1h, temp_max_past1h, temp_min_past1h
- **Grass/Soil hourly**: temp_grass_mean/min/max_past1h, temp_soil_mean/min/max_past1h
- **12-hour**: temp_min_past12h, temp_max_past12h

### 💨 **Wind (10 parameters)**
- **Current**: wind_speed, wind_dir, wind_max, wind_min
- **Hourly**: wind_speed_past1h, wind_dir_past1h, wind_min_past1h, wind_gust_always_past1h, wind_max_per10min_past1h

### 💧 **Precipitation (5 parameters)**
- **Amount**: precip_past1min, precip_past10min, precip_past1h
- **Duration**: precip_dur_past10min, precip_dur_past1h

### ☀️ **Solar Radiation (4 parameters)**
- **Radiation**: radia_glob, radia_glob_past1h
- **Sunshine**: sun_last10min_glob, sun_last1h_glob

### 🌫️ **Atmospheric (7 parameters)**
- **Pressure**: pressure, pressure_at_sea
- **Humidity**: humidity, humidity_past1h
- **Visibility**: visibility, visib_mean_last10min
- **Clouds**: cloud_cover, cloud_height

### 🌿 **Agricultural (3 parameters)**
- **Leaf wetness**: leav_hum_dur_past10min, leav_hum_dur_past1h

### 🌦️ **Other (2 parameters)**
- **Weather condition**: weather (code/description)

---

## Recommendations for Energy ML Model

### ✅ **High Priority (Most Relevant for Energy Prediction)**
1. **temp_dry** (13.5M obs) - Affects heating/cooling demand, solar efficiency
2. **wind_speed** (12.5M obs) - **Critical for wind energy production**
3. **wind_dir** (12.5M obs) - Wind turbine orientation
4. **humidity** (13.5M obs) - Solar panel efficiency, thermal comfort
5. **radia_glob** (6.0M obs) - **Critical for solar energy production**
6. **cloud_cover** (4.7M obs) - Affects solar production
7. **pressure** (11.9M obs) - Weather patterns
8. **temp_dew** (13.5M obs) - Moisture, affects energy systems

### ⚠️ **Medium Priority (Useful but Less Critical)**
- wind_max, wind_min (wind variability)
- sun_last10min_glob, sun_last1h_glob (direct sunlight)
- precip_past10min, precip_past1h (affects solar, grid demand)
- visibility (fog affects solar)

### 📊 **Low Priority (Agricultural/Specialized)**
- temp_grass, temp_soil (agricultural use)
- leav_hum_dur_* (agricultural sensors)
- All "past12h" parameters (less granular)

---

## Data Quality Note

**Coverage varies widely**:
- Top 8 parameters: 6-7% coverage (11-13M observations each)
- Middle tier: 1-6% coverage (2-12M observations)
- Bottom tier: <1% coverage (200K-800K observations)

**Recommendation**: Focus on top 10-15 parameters with best coverage for ML model training.
