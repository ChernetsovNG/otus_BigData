CREATE DATABASE IF NOT EXISTS clickstream;
CREATE TABLE IF NOT EXISTS clickstream.clickstream (
	referrer string,
	object string,
	link_type string,
	count int
)
comment 'clickstream table'
row format delimited
fields terminated by '\t';

load data inpath '/data/second_month/clickstream-enwiki-2017-12.tsv.gz'
overwrite into table clickstream.clickstream;

CREATE EXTERNAL TABLE IF NOT EXISTS clickstream.clickstream_external (
    referrer string,
    object string,
    link_type string,
    count int
)
comment 'clickstream table'
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '/data/first_month/';