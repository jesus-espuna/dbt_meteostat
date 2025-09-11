/*
we want to see for each airport daily:
only the airports we collected the weather data for
unique number of departures connections
unique number of arrival connections
how many flight were planned in total (departures & arrivals)
how many flights were canceled in total (departures & arrivals)
how many flights were diverted in total (departures & arrivals)
how many flights actually occured in total (departures & arrivals)
(optional) how many unique airplanes travelled on average
(optional) how many unique airlines were in service on average
(optional) add city, country and name of the airport
daily min temperature
daily max temperature
daily precipitation
daily snow fall
daily average wind direction
daily average wind speed
daily wnd peakgust


*/

WITH departures AS (
					SELECT origin AS airport_code
							, flight_date AS dep_date
							, count (DISTINCT dest) AS nunique_to -- unique number of departures connections
							, count(sched_dep_time) AS dep_planned -- how many flight were planned 
							, sum(cancelled) AS dep_cancelled
							, sum(diverted) AS dep_diverted
							, count(dep_time) AS dep_n_flights
					FROM {{ref('prep_flights')}} AS d
					INNER JOIN prep_weather_daily AS wd
					ON d.origin = wd.airport_code
					GROUP BY origin
							, dep_date
),
arrivals AS (
			SELECT dest AS airport_code
					, count (DISTINCT origin) AS nunique_from
					, count(sched_arr_time) AS arr_planned -- how many flight were planned
					, sum(cancelled) AS arr_cancelled
					, sum(diverted) AS arr_diverted
					, count(arr_time) AS arr_n_flights
			FROM {{ref('prep_flights')}} AS a
			INNER JOIN {{ref('prep_weather_daily')}} AS wd
			ON a.dest = wd.airport_code
			GROUP BY dest
),
total_stats AS (
				SELECT d.airport_code
						, d.dep_date
						, nunique_to
						, nunique_from
						, (dep_planned + arr_planned) as total_planned
						, (dep_diverted + arr_diverted) AS total_diverted
						, (dep_cancelled + arr_cancelled) AS total_cancelled
						, (dep_n_flights + arr_n_flights) AS total_flights
				FROM departures d
				JOIN arrivals a 
				ON d.airport_code = a.airport_code
)
SELECT ts.*
		, wd.min_temp_c AS daily_min_temp
		, wd.max_temp_c AS daily_max_temp
		, wd.precipitation_mm AS daily_precipitation
		, wd.max_snow_mm AS daily_snow
		, wd.avg_wind_direction AS wind_direction
		, wd.avg_wind_speed_kmh AS wind_speed
		, wd.wind_peakgust_kmh AS wind_gusts
FROM total_stats AS ts
JOIN {{ref('prep_weather_daily')}} AS wd
ON ts.airport_code = wd.airport_code