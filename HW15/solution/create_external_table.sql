DROP TABLE IF EXISTS raw.video_game_sales_external;

CREATE DATABASE IF NOT EXISTS raw;

CREATE EXTERNAL TABLE IF NOT EXISTS raw.video_game_sales_external (
    Name string,
    Platform string,
    Year_of_Release int,
    Genre string,
    Publisher string,
    NA_Sales double,
    EU_Sales double,
    JP_Sales double,
    Other_Sales double,
    Global_Sales double,
    Critic_Score int,
    Critic_Count int,
    User_Score int,
    User_Count int,
    Developer string,
    Rating string
)
comment 'video game sales table'
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\""
)
STORED AS TEXTFILE
location '/HW15/data/'
tblproperties ("skip.header.line.count"="1");

-- Запрос из таблицы
SELECT * FROM raw.video_game_sales_external LIMIT 10;

OK
Wii Sports	Wii	2006	Sports	Nintendo	41.36	28.96	3.77	8.45	82.53	76	51	8	322	NintendoE
Super Mario Bros.	NES	1985	Platform	Nintendo	29.08	3.58	6.81	0.77	40.24				
Mario Kart Wii	Wii	2008	Racing	Nintendo	15.68	12.76	3.79	3.29	35.52	82	73	8.3	709	NintendoE
Wii Sports Resort	Wii	2009	Sports	Nintendo	15.61	10.93	3.28	2.95	32.77	80	73	8	192	Nintendo	E
Pokemon Red/Pokemon Blue	GB	1996	Role-Playing	Nintendo	11.27	8.89	10.22	1	31.37			
Tetris	GB	1989	Puzzle	Nintendo	23.2	2.26	4.22	0.58	30.26						
New Super Mario Bros.	DS	2006	Platform	Nintendo	11.28	9.14	6.5	2.88	29.8	89	65	8.5	431	Nintendo	E
Wii Play	Wii	2006	Misc	Nintendo	13.96	9.18	2.93	2.84	28.92	58	41	6.6	129	NintendoE
New Super Mario Bros. Wii	Wii	2009	Platform	Nintendo	14.44	6.94	4.7	2.24	28.32	87	80	8.4	594	Nintendo	E
Duck Hunt	NES	1984	Shooter	Nintendo	26.93	0.63	0.28	0.47	28.31						
Time taken: 0.139 seconds, Fetched: 10 row(s)