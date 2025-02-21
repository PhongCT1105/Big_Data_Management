data_page = LOAD 'shared_folder/Project2/pages.csv' USING PigStorage(',') AS (id:chararray, name:chararray, nationality:chararray, country_code:int, hobby:chararray);
data_access_log = LOAD 'shared_folder/Project2/access_logs.csv' USING PigStorage(',') AS (accessid:chararray, userid:chararray, targetid:chararray, access_type:int, time:chararray);

group_access_log = GROUP data_access_log BY targetid;
page_views = FOREACH group_access_log GENERATE group AS targetid, COUNT (data_access_log) as count;

ordered_pages = ORDER page_views BY count DESC;
top_ten = LIMIT ordered_pages 10;

joined_data = JOIN data_page by TRIM(id), top_ten by TRIM(targetid);

result = FOREACH joined_data GENERATE id, name, hobby;

STORE result INTO 'output/TaskB' USING PigStorage(',');