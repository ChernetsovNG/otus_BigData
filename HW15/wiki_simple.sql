CREATE TABLE IF NOT EXISTS test_database.wiki_simple (
    name_1 string,
    name_2 string,
    dt_text string,
    count int
)
row format delimited
fields terminated by ',';

load data local inpath '/home/n_chernetsov/Dropbox/Education/Otus_BigData/otus_BigData/HW15/cl_data.txt'
overwrite into table test_database.wiki_simple;