SELECT *
FROM prep_weather_daily;

SELECT CW 	
		, avg(avg_temp_c) AS avg_temperature
		, min(min_temp_c) AS min_temperature
		, max(max_temp_c) AS max_temperature
		, avg(PRECIPITATION_MM) AS avg_precipitation_mm
		, max(max_snow_mm) AS max_snow_mm
		, avg(avg_wind_direction) AS avg_wind_direction
		, max(AVG_WIND_SPEED_KMH) AS max_avg_wind_speed
		, max(WIND_PEAKGUST_KMH) AS max_wind_peakgust
		, avg(avg_pressure_hpa) AS AVG_PRESSURE_HPA 
		, avg(sun_minutes) AS avg_sun_minutes
FROM {{ref('prep_weather_daily')}}
GROUP BY cw
ORDER BY cw