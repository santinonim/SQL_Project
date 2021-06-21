/* The country names differed on the tables, therefore it was important that:
 * (1) the tables were joined through country code ISO3 and 
 * (2) all the 190 countries from the table COVID19 were in the final table.
 */

/* ************* FIRST TABLE - COUNTRIES *************** 
 * The table joins data from tables: Lookup table, Life Expectancy, Countries a Economies
 * Firstly I join the tables lookup_table and Countries on ISO3, because the names of the countries in lookup_table are the same
 * as in the table COVID19. 
 * This way, I can assure that the resulting table will include information for all 190 countries from the COVID19 table
 * and that the join with other tables will be made on ISO3, which reduces the risk of losing information due to different country names */
Create table t_mariana_santinoni_sqlproject_countries AS
SELECT a.country as country_lt, a.iso3, c.country as country_countries, round(c.population_density,2) , c.population , c.median_age_2018,
	life.life_exp_difference, eco.Gini_2016, eco.Mortality_under5_2019, eco.GDP_per_capita_2019
FROM (select country, iso3 from lookup_table lt where province is NULL) a
left join countries c 
on a.iso3 = c.iso3
/* Left join with a query from table life_expectancy so that it shows the difference of life expectancy between 1965 and 2015.
*/
LEFT JOIN (SELECT a.country, b.iso3,
			round( b.life_exp_2015 - a.life_exp_1965, 2 ) as life_exp_difference
			FROM (
			    SELECT le.country , le.life_expectancy as life_exp_1965
			    FROM life_expectancy le 
			    WHERE year = 1965
			    and iso3 is not NULL 
			    ) a 
			JOIN (
			    SELECT le.country , le.iso3, le.life_expectancy as life_exp_2015
			    FROM life_expectancy le 
			    WHERE year = 2015
			    ) b
			ON a.country = b.country) life
on a.iso3 = life.iso3
/* Left join with a query from the table economies. The table economies does not have the column ISO3, therefore the join
 * had to be made on the country name.
 * The query on the table economies shows Mortality under 5 and GDP per capita from the year 2019
 * but GINI coefficient from 2016.
 * There are two reasons for such approach: (1) data on GINI coefficient from 2020 and 2019 are not available and the data for 2018 
 * is only available for 29 countries, meanwhile the data for 2016 is available for 76 countries, and 
 * (2) changes on GINI coefficient are not usually significant on a y-o-y comparison, but more likely between longer periods of time,
 * therefore data from 2016 can be relevant.
 */
left join (SELECT e.country, e.gini as Gini_2016, e2.Mortality_under5_2019, e2.GDP_per_capita_2019
			FROM economies e 
			join (SELECT country , ROUND(mortaliy_under5, 2) as Mortality_under5_2019, round(GDP / population, 2) as GDP_per_capita_2019
				FROM economies e 
				where `year` = 2019
				GROUP by country) e2
			on e.country = e2.country
			where `year` = 2016) eco
on c.country = eco.country
;


/* ******************* SECOND TABLE - Religions *************************
 * The MAX aggregation function was used in order to have each religion as a single column.
 * The table joins several subqueries with the table countries so that the resulting table contains the column ISO3
 *  */
CREATE TABLE t_mariana_santinoni_sqlproject_religions AS
SELECT r3.country, c2.iso3, 
MAX(CASE WHEN r3.religion = 'Islam' THEN religion_share_2020 END) AS Islam_share,
MAX(CASE WHEN r3.religion = 'Christianity' THEN religion_share_2020 END) AS Christianity_share,
MAX(CASE WHEN r3.religion = 'Unaffiliated Religions' THEN religion_share_2020 END) AS Unaffiliated_Religions_share,
MAX(CASE WHEN r3.religion = 'Hinduism' THEN religion_share_2020 END) AS Hinduism_share,
MAX(CASE WHEN r3.religion = 'Buddhism' THEN religion_share_2020 END) AS Buddhism_share,
MAX(CASE WHEN r3.religion = 'Folk Religions' THEN religion_share_2020 END) AS Folk_Religions_share,
MAX(CASE WHEN r3.religion = 'Other Religions' THEN religion_share_2020 END) AS Other_Religions_share,
MAX(CASE WHEN r3.religion = 'Judaism' THEN religion_share_2020 END) AS Judaism_share
/* The subquery on the JOIN clause is the calculation of total population. With this number it is then 
 * possible calculate the share of each religion for the year 2020 */
FROM (SELECT r.country , 
	r.religion , 
    ROUND( r.population / r2.total_population_2020 * 100, 2 ) AS religion_share_2020
	FROM religions r 
	JOIN (
        SELECT r.country, r.`year`, SUM(r.population) AS total_population_2020
        FROM religions r 
        WHERE r.year = 2020 AND r.country != 'All Countries' AND r.population > 0
        GROUP BY r.country
    	) r2
	ON r.country = r2.country
	AND r.year = r2.year) r3
LEFT JOIN countries c2 
ON r3.country = c2.country 
GROUP BY r3.country

/* ******************* THIRD TABLE - WEATHER *************************
 * The table joins several subqueries with the table weather and countries, so that the resulting table contains the column ISO3
 * */
CREATE TABLE t_mariana_santinoni_sqlproject_weather AS
SELECT c2.country,
	c2.iso3,
	w.`date` , 
	MAX(w.gust) AS max_wind, 
	w2.Avg_daily_temp, 
	CASE WHEN w3.number_of_rainy_hours IS NULL THEN 0 ELSE w3.number_of_rainy_hours END AS Number_of_rainy_hours
FROM weather w 
/* The subquery calculates the average daily temperature. It includes only the dates for which there is data on COVID*/
JOIN (SELECT city ,
		`date` , 
		AVG(temp) AS Avg_daily_temp
	FROM weather w 
	WHERE `hour` IN (6,9,12,15,18)
	AND `date` BETWEEN '20200122' AND '20201109'
	GROUP BY city, `date`) w2
ON w.city = w2.city
AND w.`date` = w2.`date`
/* The subquery calculates the number of hours of rain. LEFT JOIN was used so that the resulting table would also include the dates when it did not rain*/
LEFT JOIN (SELECT city ,
			`date` , 
			COUNT(rain) * 3 AS number_of_rainy_hours
		FROM weather w 
		WHERE rain >0
		AND `date` BETWEEN '20200122' AND '20201109'
		GROUP BY city, `date` ) w3
ON w2.city = w3.city
AND w2.`date` = w3.`date`
/* The subquery is a selection from the table countries so that the resulting table has country names, not city names 
 * and in order to add ISO3 */
JOIN (SELECT capital_city, iso3, country
			FROM countries c) c2
ON w2.city = c2.capital_city
GROUP BY w.city, w.`date` 



/* ******************* FINAL TABLE **************
 * The table joins all previously created tables with data from COVID19_differences, COVID19_tests.
 * The table also contains columns for weekend-weekday and season 
 * */

create table t_mariana_santinoni_projekt_SQL_final
SELECT
	cbd.country ,
	cbd.`date` ,
	case
		when WEEKDAY(cbd.`date`) in (5,	6) then 1
		else 0
	end as weekend,
	case
		when month(cbd.`date`) in (3,4,	5) then 0
		when month(cbd.`date`) in (6, 7, 8) then 1
		when month(cbd.`date`) in (9, 10, 11) then 2
		else 3
	end as season,
	cbd.confirmed,
	ct.tests_performed ,
	tmssc.population,
	tmssc.`round(c.population_density,2)` as population_density,
	tmssc.GDP_per_capita_2019,
	tmssc.Gini_2016,
	tmssc.life_exp_difference,
	tmssc.median_age_2018,
	tmssc.Mortality_under5_2019,
	tmssr.Buddhism_share ,
	tmssr.Christianity_share ,
	tmssr.Folk_Religions_share ,
	tmssr.Hinduism_share ,
	tmssr.Islam_share ,
	tmssr.Judaism_share ,
	tmssr.Other_Religions_share ,
	tmssr.Unaffiliated_Religions_share ,
	tmssw.Avg_daily_temp ,
	tmssw.max_wind ,
	tmssw.Number_of_rainy_hours
from
	covid19_basic_differences cbd
/*LEFT JOIN on country name because Covid table does not have ISO3
 * The join is made on the column country_lt, ie from the column where the country names are as in the Covid table*/
left join t_mariana_santinoni_sqlproject_countries tmssc on
	cbd.country = tmssc.country_lt
/*LEFT JOIN with a subquery from the table COVID19_tests, because for some countries (namely France, India, Italy, Japan, Poland, Singapore and US)
 * there are two different data for testing. This subquery filters only 'tests performed' */
left join (
	select
		ct1.ISO,
		ct1.date,
		ct1.tests_performed
	from
		covid19_tests ct1
	where
		ct1.entity = 'tests performed') ct on
	tmssc.iso3 = ct.ISO
	and cbd.`date` = ct.`date`
/*JOIN with my religion table*/
left join t_mariana_santinoni_sqlproject_religions tmssr on
	tmssc.iso3 = tmssr.iso3
/*OIN with my weather table*/
left join t_mariana_santinoni_sqlproject_weather tmssw on
	tmssc.iso3 = tmssw.iso3
	and cbd.`date` = tmssw.`date`
order by
	cbd.country ASC

/*The result is a tabular table. There is information for 190 countries. Each country contains information for 293 dates.
 * All data from the table COVID19_differences was kept and because of using the country code ISO3 the loss of information by joining tables
 * was minimal.
 *  Missing data could be added additionally. */
	
