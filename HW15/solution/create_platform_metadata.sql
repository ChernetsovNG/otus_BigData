
-- для начала узнаем, какие у нас есть платформы
SELECT DISTINCT platform FROM ods.video_game_sales_orc;

-- получаем следующий список
2600, 3DO, 3DS, DC, DS, GB, GBA, GC, GEN, GG, N64, NES, NG
PC, PCFX, PS, PS2, PS3, PS4, PSP, PSV, SAT, SCD, SNES, TG16
WS, Wii, WiiU, X360, XB, XOne

DROP TABLE IF EXISTS md.platform;

CREATE DATABASE IF NOT EXISTS md;

CREATE EXTERNAL TABLE IF NOT EXISTS md.platform (
    platform string,
    id int
)
comment 'video game sales orc table partitioned by year of release'
row format delimited
fields terminated by ',';

-- Чтобы использовать автоинкрементное поле, делаем следующее
hive> add jar /home/n_chernetsov/ProgramFiles/Hive/apache-hive-2.3.2-bin/lib/hive-contrib-2.3.2.jar;
hive> create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

INSERT INTO TABLE md.platform SELECT platform, row_sequence() FROM ods.video_game_sales_orc GROUP BY platform;

-- Данные в таблице md.platform
SELECT * FROM md.platform;

OK
2600	1
3DO	2
3DS	3
DC	4
DS	5
GB	6
GBA	7
GC	8
GEN	9
GG	10
N64	11
NES	12
NG	13
PC	14
PCFX	15
PS	16
PS2	17
PS3	18
PS4	19
PSP	20
PSV	21
SAT	22
SCD	23
SNES	24
TG16	25
WS	26
Wii	27
WiiU	28
X360	29
XB	30
XOne	31
Time taken: 0.115 seconds, Fetched: 31 row(s)