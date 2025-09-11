/*
 origin airport code
destination airport code
total flights on this route
unique airplanes
unique airlines
on average what is the actual elapsed time
on average what is the delay on arrival
what was the max delay?
what was the min delay?
total number of cancelled
total number of diverted
add city, country and name for both, origin and destination, airports

 */

WITH routes AS (
				SELECT origin 
						,dest 
						, count(*) AS n_flights
						, count(DISTINCT tail_number) AS n_planes
						, count(DISTINCT airline) AS n_airlines
						, avg(ACTUAL_ELAPSED_TIME_INTERVAL) AS avg_elapsed_time
						, avg(arr_delay_interval) AS avg_arrival_delay
						, max(arr_delay_interval) AS max_arrival_delay
						, min(arr_delay_interval) AS min_arrival_delay
						, sum(cancelled) AS n_cancelled_flights
						, sum(diverted) AS n_diverted_flights
				FROM {{ref('prep_flights')}}
				GROUP BY ORIGIN 
						, dest
)
SELECT r.origin AS origin_airport_code
		, ao.name AS origin_airport_name
		, ao.city AS origin_airport_city
		, ao.country AS origin_aiport_country
		,r.dest AS destination_airport_code
		, ad.name AS destination_airport_name
		, ad.city AS destination_airport_city
		, ad.country AS destination_aiport_country
		,r.n_flights
		,r.n_planes
		,r.n_airlines
		,r.avg_elapsed_time
		,r.avg_arrival_delay
		,r.max_arrival_delay
		,r.min_arrival_delay
		,r.n_cancelled_flights
		,r.n_diverted_flights
FROM routes AS r
JOIN {{ref('prep_airports')}} AS ao
ON r.origin = ao.faa
JOIN {{ref('prep_airports')}} AS ad
ON r.dest = ad.faa
