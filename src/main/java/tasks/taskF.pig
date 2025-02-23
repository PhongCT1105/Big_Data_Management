-- Load the friends dataset
friends = LOAD 'shared_folder/friends.csv' USING PigStorage(',') AS (FriendRel:chararray, PersonID:int, MyFriend:int, DateOfFriendship:chararray, Description:chararray);

-- Load the access_logs dataset
access_logs = LOAD 'shared_folder/access_logs.csv' USING PigStorage(',') AS (accessID:int, ByWho:int, WhatPage:int, AccessTime:chararray);

-- Extract relevant columns: friendships (p1 -> p2)
friendships = FOREACH friends GENERATE PersonID AS p1, MyFriend AS p2;

-- Extract relevant columns: accesses (p1 -> p2)
accesses = FOREACH access_logs GENERATE ByWho AS p1, WhatPage AS p2;

-- Find people (p1) who have accessed their friends (p2)
p1_accessed_p2 = JOIN friendships BY (p1, p2) LEFT OUTER, accesses BY (p1, p2);

-- Filter out those who have accessed their friends
p1_without_access = FILTER p1_accessed_p2 BY accesses::p1 IS NULL;

-- Apply DISTINCT before FOREACH (fixing syntax)
neglectful_p1 = DISTINCT p1_without_access;

-- Extract only `PersonID`
neglectful_p1 = FOREACH neglectful_p1 GENERATE friendships::p1 AS PersonID;

-- Load persons dataset to get names
persons = LOAD 'shared_folder/pages.csv' USING PigStorage(',') AS (PersonID:int, Name:chararray);

-- Join with persons dataset to get names
result = JOIN neglectful_p1 BY PersonID, persons BY PersonID;

-- Extract only required fields
result = FOREACH result GENERATE neglectful_p1::PersonID AS PersonID, persons::Name AS Name;

-- Remove duplicates from final result
result = DISTINCT result;

-- Store the final output
STORE result INTO 'shared_folder/pigF' USING PigStorage(',');
