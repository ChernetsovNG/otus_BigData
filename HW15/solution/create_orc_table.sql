DROP TABLE IF EXISTS ods.video_game_sales_orc;

CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.video_game_sales_orc (
	Name string,
    Platform string,
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
comment 'video game sales orc table partitioned by year of release'
PARTITIONED BY (year int)
row format delimited
fields terminated by ','
stored as orc
tblproperties ("orc.compress"="ZLIB");