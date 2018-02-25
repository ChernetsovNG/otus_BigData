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

LOAD DATA INPATH '/data/second_month/clickstream-enwiki-2017-12.tsv.gz'
OVERWRITE INTO TABLE clickstream.clickstream;