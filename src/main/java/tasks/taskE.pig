-- Load the access log data
data_access_log = LOAD 'shared_folder/Project2/access_logs.csv' USING PigStorage(',')
    AS (logid:chararray, userid:chararray, targetid:chararray, access_type:int, time:chararray);

-- Group the data by userid
grouped_data = GROUP data_access_log BY userid;

-- Process each group
result = FOREACH grouped_data {
    -- Calculate total accesses for each user
    total_accesses = COUNT(data_access_log);

    -- Generate distinct pages accessed by the user
    distinct_pages = DISTINCT data_access_log.targetid;

    -- Count the number of distinct pages
    distinct_page_count = COUNT(distinct_pages);

    -- Output the results
    GENERATE group AS person_id, total_accesses, distinct_page_count;
};

-- Store the result
STORE result INTO 'output/taskE' USING PigStorage(',');