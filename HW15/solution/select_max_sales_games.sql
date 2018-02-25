-- Выводим игру с максимальными продажами за каждый год
-- Во внутреннем запросе делаем группировку по году и определяем максимальную цену,
-- затем соединяем во внешнем подзапросе для нахождения названия игры и платформы,
-- а затем объединяем со справочником платформ, чтобы найти название платформы

SELECT out_query.name, md.platform, out_query.year, out_query.sales
FROM ads.eu_sales AS out_query
INNER JOIN (
    SELECT in_query.year AS year, MAX(in_query.sales) AS sales
    FROM ads.eu_sales AS in_query
    WHERE in_query.year IS NOT NULL AND in_query.platform_id IS NOT NULL
    GROUP BY in_query.year
) subquery_result ON out_query.year = subquery_result.year AND out_query.sales = subquery_result.sales
AND out_query.year IS NOT NULL AND out_query.platform_id IS NOT NULL
INNER JOIN md.platform AS md ON out_query.platform_id = md.id;

-- Результат выполнения запроса:

OK
Asteroids	2600	1980	0.26
Pitfall!	2600	1981	0.24
Pac-Man	2600	1982	0.45
Mario Bros.	NES	1983	0.12
Popeye	NES	1983	0.12
Duck Hunt	NES	1984	0.63
Super Mario Bros.	NES	1985	3.58
The Legend of Zelda	NES	1986	0.93
Zelda II: The Adventure of Link	NES	1987	0.5
Super Mario Bros. 3	NES	1988	3.44
Super Mario Land	GB	1989	2.71
Super Mario World	SNES	1990	3.75
The Legend of Zelda: A Link to the Past	SNES	1991	0.91
Sonic the Hedgehog	GEN	1991	0.91
Super Mario Land 2: 6 Golden Coins	GB	1992	2.04
Super Mario All-Stars	SNES	1993	2.15
Myst	PC	1994	2.79
Warcraft II: Tides of Darkness	PC	1995	2.27
Pokemon Red/Pokemon Blue	GB	1996	8.89
Gran Turismo	PS	1997	3.87
Pokémon Yellow: Special Pikachu Edition	GB	1998	5.04
Pokemon Gold/Pokemon Silver	GB	1999	6.18
Driver 2	PS	2000	2.1
Gran Turismo 3: A-Spec	PS2	2001	5.09
Grand Theft Auto: Vice City	PS2	2002	5.49
Need for Speed Underground	PS2	2003	2.83
World of Warcraft	PC	2004	6.21
Nintendogs	DS	2005	10.95
Wii Sports	Wii	2006	28.96
Wii Fit	Wii	2007	8.03
Mario Kart Wii	Wii	2008	12.76
Wii Sports Resort	Wii	2009	10.93
Kinect Adventures!	X360	2010	4.89
Call of Duty: Modern Warfare 3	PS3	2011	5.73
Call of Duty: Black Ops II	PS3	2012	5.73
Grand Theft Auto V	PS3	2013	9.09
Grand Theft Auto V	PS4	2014	6.31
FIFA 16	PS4	2015	6.12
FIFA 17	PS4	2016	5.75
Phantasy Star Online 2 Episode 4: Deluxe Package	PS4	2017	0.0
Phantasy Star Online 2 Episode 4: Deluxe Package	PSV	2017	0.0
Brothers Conflict: Precious Baby	PSV	2017	0.0
Imagine: Makeup Artist	DS	2020	0.0
Time taken: 39.232 seconds, Fetched: 43 row(s)