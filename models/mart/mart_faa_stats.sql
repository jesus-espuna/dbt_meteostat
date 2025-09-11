SELECT * FROM prep_flights;


/*
unique number of departures connections

unique number of arrival connections

how many flight were planned in total (departures & arrivals)

how many flights were canceled in total (departures & arrivals)

how many flights were diverted in total (departures & arrivals)

how many flights actually occured in total (departures & arrivals)

(optional) how many unique airplanes travelled on average

(optional) how many unique airlines were in service on average

add city, country and name of the airport

*/

WITH departures AS (
					SELECT origin AS airport_code
							, count (DISTINCT dest) AS nunique_to -- unique number of departures connections
							, count(sched_dep_time) AS dep_planned -- how many flight were planned 
							, sum(cancelled) AS dep_cancelled
							, sum(diverted) AS dep_diverted
							, count(dep_time) AS dep_n_flights
					FROM {{REF('prep_flights')}}
					GROUP BY origin
),
arrivals AS (
			SELECT dest AS airport_code
					, count (DISTINCT origin) AS nunique_from
					, count(sched_arr_time) AS arr_planned -- how many flight were planned
					, sum(cancelled) AS arr_cancelled
					, sum(diverted) AS arr_diverted
					, count(arr_time) AS arr_n_flights
			FROM {{REF('prep_flights')}}
			GROUP BY dest
),
total_stats AS (
				SELECT d.airport_code
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
SELECT a.city, a.country, a.name, 
		ts.*
FROM total_stats as ts
JOIN {{REF('PREP_AIRPORTS')}} AS a
ON airport_code = faa
				