-- Перед вставкой в таблицу необходимо задать следующие свойства
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE ods.video_game_sales_orc partition (year)  
SELECT name, platform, genre, publisher, na_sales, eu_sales, 
       jp_sales, other_sales, global_sales, critic_score, critic_count,
       user_score, user_count, developer, rating, year_of_release
FROM raw.video_game_sales_external;

-- Запрос из таблицы orc
SELECT * FROM ods.video_game_sales_orc LIMIT 10;

OK
Asteroids	2600	Shooter	Atari	4.0	0.26	0.0	0.05	4.31	NULL	NULL	NULL	NULL			1980
Missile Command	2600	Shooter	Atari	2.56	0.17	0.0	0.03	2.76	NULL	NULL	NULL	NULL			1980
Kaboom!	2600	Misc	Activision	1.07	0.07	0.0	0.01	1.15	NULL	NULL	NULL	NULL			1980
Defender	2600	Misc	Atari	0.99	0.05	0.0	0.01	1.05	NULL	NULL	NULL	NULL			1980
Boxing	2600	Fighting	Activision	0.72	0.04	0.0	0.01	0.77	NULL	NULL	NULL	NULL			1980
Ice Hockey	2600	Sports	Activision	0.46	0.03	0.0	0.01	0.49	NULL	NULL	NULL	NULL			1980
Freeway	2600	Action	Activision	0.32	0.02	0.0	0.0	0.34	NULL	NULL	NULL	NULL			1980
Bridge	2600	Misc	Activision	0.25	0.02	0.0	0.0	0.27	NULL	NULL	NULL	NULL			1980
Checkers	2600	Misc	Atari	0.22	0.01	0.0	0.0	0.24	NULL	NULL	NULL	NULL			1980
Pitfall!	2600	Platform	Activision	4.21	0.24	0.0	0.05	4.5	NULL	NULL	NULL	NULL		1981
Time taken: 0.138 seconds, Fetched: 10 row(s)