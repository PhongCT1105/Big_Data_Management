data = LOAD 'shared_folder/Project2/pages.csv' USING PigStorage(',') AS (id:chararray, name:chararray, nationality:chararray, country_code:int, hobby:chararray);

filtered_users = FILTER data BY nationality == 'France';

-- Select only the name and hobby
result = FOREACH filtered_users GENERATE name, hobby;

-- Store the output
STORE result INTO 'output/TaskA' USING PigStorage(',');

