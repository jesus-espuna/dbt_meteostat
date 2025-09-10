WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, date_part('day', date) AS date_day 		-- number of the day of month
		, date_part('month', date) AS date_month 	-- number of the month of year
		, date_part('year', date) AS date_year 		-- number of year
		, date_part('week', date) AS cw 			-- number of the week of year
		, trim(to_char(date, 'Month')) AS month_name 	-- name of the month
		, trim(to_char(date, 'Day')) AS weekday 		-- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_name in ('December', 'January', 'February') THEN 'winter'
			WHEN month_name IN ('March','April','May') THEN 'spring'
            WHEN month_name IN ('June','July','August') THEN 'summer'
            WHEN month_name IN ('September','October','November') THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date