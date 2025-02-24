REGISTER 'C:/tools/pig/contrib/piggybank/java/piggybank.jar';

data_access_log = LOAD 'access_logs.csv' USING PigStorage(',')
    AS (accessid:chararray, userid:chararray, targetid:chararray, accesstype:chararray, time:chararray);

data_access_log = FOREACH data_access_log GENERATE
    accessid, userid, targetid, accesstype,
    ToDate(time, 'yyyy-MM-dd HH:mm:ss') AS (time:DateTime);

grouped_data = GROUP data_access_log BY userid;

-- Find the latest access time for each user
latest_access = FOREACH grouped_data {
    latest_time = MAX(data_access_log.time);
    GENERATE group AS person_id, latest_time;
};

-- Define the reference date (current date or a specific date)
-- For example, let's assume the reference date is 2024-06-21
reference_date = ToDate('2024-06-21', 'yyyy-MM-dd');

disconnected_users = FOREACH latest_access GENERATE
    person_id,
    latest_time,
    DaysBetween(reference_date, latest_time) AS days_since_last_access;

disconnected_users = FILTER disconnected_users BY days_since_last_access >= 14;

pages_data = LOAD 'pages.csv' USING PigStorage(',')
    AS (userid:chararray, name:chararray, nationality:chararray, country_code:chararray, hobby:chararray);

disconnected_users_with_names = JOIN disconnected_users BY person_id, pages_data BY PersonID;

result = FOREACH disconnected_users_with_names GENERATE
    disconnected_users::person_id AS userid,
    pages_data::name AS name;

-- Store the result
STORE result INTO 'output/taskG' USING PigStorage(',');

