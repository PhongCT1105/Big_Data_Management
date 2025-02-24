data_page = LOAD 'shared_folder/Project2/pages.csv' USING PigStorage(',') AS (id:chararray, name:chararray, nationality:chararray, country_code:int, hobby:chararray);
data_friends = LOAD 'shared_folder/Project2/friends.csv' USING PigStorage(',') AS (id:chararray, userid:chararray, friendid:chararray, frienddate:datetime, descr:chararray);

group_friends = GROUP data_friends BY friendid;
friend_count_res = FOREACH group_friends GENERATE group AS user_id, COUNT (data_friends) AS friend_count;

page_friends = JOIN data_page BY id LEFT OUTER, friend_count_res BY user_id;

result = FOREACH page_friends GENERATE
            data_page::name AS name,
            (friend_count_res::friend_count IS NULL ? 0 : friend_count_res::friend_count) AS friend_count;

STORE result INTO 'output/TaskD' USING PigStorage(',');