data = LOAD 'shared_folder/Project2/pages.csv' USING PigStorage(',') AS (id:chararray, name:chararray, nationality:chararray, country_code:int, hobby:chararray);

group_by_country = GROUP data BY nationality;
country_views = FOREACH group_by_country GENERATE group as country, COUNT (data) as count;

STORE country_views INTO 'output/TaskC' USING PigStorage(',');