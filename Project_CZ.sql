/* Vzhledem k tomu, ze se názvy zemí lisily, jsem vzdy dála prednost, 
 * (1) aby tabulky byly spojeny pomocí ISO3 a (2) aby se neztratily zeme tabulky COVID19.
 */

/* ************* PRVNÍ TABULKA - COUNTRIES*************** 
 * Tabulka je spojením tabulek: Lookup table, Life Expectancy, Countries a Economies
 * Nejdríve spojím ISO3 z tabulky Lookup table s ISO3 z tabulky Countries, protože zeme v tabulce Lookup Table se shodují 
 * se zememi v tabulce COVID19. Takto mohu být jistá, že tabulka bude zahrnovat informace pro zeme, pro které jsou data o 
 * COVIDu a zároven, ze s dalsími tabulkami bude join pres ISO3, coz snizí riziko ztráty informace kvuli odlisnému názvu zeme  */
Create table t_mariana_santinoni_sqlproject_countries AS
SELECT a.country as country_lt, a.iso3, c.country as country_countries, round(c.population_density,2) , c.population , c.median_age_2018,
	life.life_exp_difference, eco.Gini_2016, eco.Mortality_under5_2019, eco.GDP_per_capita_2019
FROM (select country, iso3 from lookup_table lt where province is NULL) a
left join countries c 
on a.iso3 = c.iso3
/* Left join s upravenou tabulkou Life Expectancy, která udává rozdíl mezi ocekávanou dobou dozití 
 * v roce 1965 a v roce 2015.
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
/* Dalsí left join je s upravenou tabulkou economies. Tabulka economies nemá sloupec ISO3, tak spojeni je pres názvy zeme.
 * Tabulka economies byla upravena tak, aby zobrazila umrtnost a HDP na osobu z roku 2019, ale GINI koeficient z r. 2016.
 * Vybrala jsem tento postup, ze dvou duvodu: (1) data o GINI koeficientu na rok 2020 a 2019 nejsou a pro rok 2018 byly dostupne pouze pro 29 zemí, 
 * kdezto pro rok 2016 je tato informace dostupná pro 76 zemí a (2) zmeny v GINI koeficientu nejsou tak patrné z roku na rok, ale spíše mezi dekády,
 * tak i data z roku 2016 mohou být relevantní.
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


/* ******************* DRUHÁ TABULKA - Religions *************************
 * Aby každé naboženství se zobrazilo jako sloupec byla použita agregacní fuknce MAX.
 * Tabulka je spojení nekolika vnorených dotazu s tabulkou countries tak, aby vysledná tabulka obsahovala sloupec ISO3
 * Vysvetlení  vnorených dotazu je níže
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
/* Vnorený dotaz v klauzuli JOIN je výpocet celkové populace. Pomocí tohoto výpoctu lze spocítat
 * podíly jednotlivých náboženství pro rok 2020 */
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

/* ******************* TRETÍ TABULKA - WEATHER *************************
 * Tabulka je spojení nekolika vnorených dotazu s tabulkou weather a countries tak, aby vysledná tabulka obsahovala sloupec ISO3
 * Vysvetlení  vnorených dotazu je níže
 * */
CREATE TABLE t_mariana_santinoni_sqlproject_weather AS
SELECT c2.country,
	c2.iso3,
	w.`date` , 
	MAX(w.gust) AS max_wind, 
	w2.Avg_daily_temp, 
	CASE WHEN w3.number_of_rainy_hours IS NULL THEN 0 ELSE w3.number_of_rainy_hours END AS Number_of_rainy_hours
FROM weather w 
/* První vnorený dotaz je výpocet prumerné denní teploty. Vybrala jsem datumy, pro které máme data o COVIDu*/
JOIN (SELECT city ,
		`date` , 
		AVG(temp) AS Avg_daily_temp
	FROM weather w 
	WHERE `hour` IN (6,9,12,15,18)
	AND `date` BETWEEN '20200122' AND '20201109'
	GROUP BY city, `date`) w2
ON w.city = w2.city
AND w.`date` = w2.`date`
/* Druhý vnorený dotaz je výpocet kolika hodin pršelo. Je tam left join tak aby v dotazu nezmizely dny, kdy nepršelo*/
LEFT JOIN (SELECT city ,
			`date` , 
			COUNT(rain) * 3 AS number_of_rainy_hours
		FROM weather w 
		WHERE rain >0
		AND `date` BETWEEN '20200122' AND '20201109'
		GROUP BY city, `date` ) w3
ON w2.city = w3.city
AND w2.`date` = w3.`date`
/* Tretí vnorený dotaz je výber z tabulky countries, aby byla data pro zeme ne pro mesta a aby se mohlo pouzit ISO3 */
JOIN (SELECT capital_city, iso3, country
			FROM countries c) c2
ON w2.city = c2.capital_city
GROUP BY w.city, w.`date` 



/* ******************* VÝSLEDNÁ TABULKA - FINAL **************
 * Tabulka je spojení dríve vytvorených tabulek s COVID19_differences, COVID19_tests.
 * tabulka bude také obsahovat sloupce pro binární promennou pro víkend a informace o rocním období 
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
/*První LEFT JOIN pomocí nazvu zemí nebot tabulka COVID nemá ISO3*/
left join t_mariana_santinoni_sqlproject_countries tmssc on
	cbd.country = tmssc.country_lt
/*Druhý LEFT JOIN s vnoreným dotazem tabulky COVID19_tests, protože pro nekteré zeme (France, India, Italy, Japan, Poland, Singapore a US)
 * jsou dva ruzné údaje pro testování. Nasledujici dotaz vybíra pouze data pro údaj 'tests performed' */
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
/*Spojeni s tabulkou religions*/
left join t_mariana_santinoni_sqlproject_religions tmssr on
	tmssc.iso3 = tmssr.iso3
/*Spojeni s tabulkou weather*/
left join t_mariana_santinoni_sqlproject_weather tmssw on
	tmssc.iso3 = tmssw.iso3
	and cbd.`date` = tmssw.`date`
order by
	cbd.country ASC

/*Vysledná tabulka je panelová tabulka. Obsahuje informace pro 190 zemí. Každá zema má data pro 293 datumy.
 * Informace z tabulky COVID19_differences byly zcela zachovány a pomocí použití ISO3 byla ztráta informace z jiných tabulek minimální.
 * Chybejici data lze dodatecne vyplnovat. */
	
