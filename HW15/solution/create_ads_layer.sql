DROP TABLE IF EXISTS ads.eu_sales;

CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.eu_sales (
    name string,
    platform_id int,
    year int,
    sales double
)
comment 'video game Europe sales'
row format delimited
fields terminated by ',';

-- заполняем таблицу данными по продажам в Европе
INSERT OVERWRITE TABLE ads.eu_sales  
SELECT orc.name, md.id, orc.year, orc.eu_sales
FROM ods.video_game_sales_orc AS orc JOIN md.platform AS md
ON orc.Platform = md.platform AND orc.year IS NOT NULL AND orc.platform IS NOT NULL;

-- Запрос из таблицы ads
SELECT * FROM ads.eu_sales LIMIT 10;

OK
Asteroids	1	1980	0.26
Missile Command	1	1980	0.17
Kaboom!	1	1980	0.07
Defender	1	1980	0.05
Boxing	1	1980	0.04
Ice Hockey	1	1980	0.03
Freeway	1	1980	0.02
Bridge	1	1980	0.02
Checkers	1	1980	0.01
Pitfall!	1	1981	0.24
Time taken: 0.127 seconds, Fetched: 10 row(s)