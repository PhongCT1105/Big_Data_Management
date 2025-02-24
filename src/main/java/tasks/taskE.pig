-- Load the access log data
data_access_log = LOAD 'shared_folder/Project2/access_logs.csv' USING PigStorage(',') AS (logid:chararray, userid:chararray, targetid:chararray, access_type:int, time:datetime);

-- Group the data by person_id
grouped_data = GROUP data_access_log BY userid;

-- Calculate the total accesses and distinct pages accessed by each person
result = FOREACH grouped_data {
    -- Count total accesses (number of records per person)
    total_accesses = COUNT(data_access_log);

    -- Get distinct pages accessed by each person
    distinct_pages = DISTINCT data_access_log::targetid;

    -- Count the number of distinct pages
    distinct_page_count = COUNT(distinct_pages);

    -- Output the person_id, total_accesses, and distinct_page_count
    GENERATE group AS person_id, total_accesses, distinct_page_count;
};

-- Store the result in the output directory
STORE result INTO 'output/taskE' USING PigStorage(',');