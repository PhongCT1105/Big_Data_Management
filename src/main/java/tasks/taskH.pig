-- Load the friends dataset
friends = LOAD 'shared_folder/friends.csv' USING PigStorage(',') AS (FriendRel:chararray, PersonID:int, MyFriend:int, DateOfFriendship:chararray, Description:chararray);

-- Count the number of friendships per person (p1)
friend_counts = GROUP friends BY PersonID;
friend_counts = FOREACH friend_counts GENERATE group AS PersonID, COUNT(friends) AS num_friends;

-- Compute the total number of friendships and number of unique people
total_stats = GROUP friend_counts ALL;
total_stats = FOREACH total_stats GENERATE 
    SUM(friend_counts.num_friends) AS total_friends, 
    COUNT(friend_counts) AS num_people;

-- Compute the average number of friendships per person
average_friends = FOREACH total_stats GENERATE 
    (double) total_friends / num_people AS avg_friends;

-- Ensure average_friends has a single value for all records (cross join to make it accessible)
crossed_data = CROSS friend_counts, average_friends;

-- Filter people who have more friendships than the average
popular_people = FILTER crossed_data BY friend_counts::num_friends > average_friends::avg_friends;
popular_people = FOREACH popular_people GENERATE friend_counts::PersonID AS PersonID;

-- Load pages dataset to get names
pages = LOAD 'shared_folder/pages.csv' USING PigStorage(',') AS (PersonID:int, Name:chararray, Nationality:chararray, CountryCode:chararray, Hobby:chararray);

-- Join with pages to get names of popular people
result = JOIN popular_people BY PersonID, pages BY PersonID;

-- Extract required fields
result = FOREACH result GENERATE popular_people::PersonID AS PersonID, pages::Name AS Name;

-- Remove duplicates
result = DISTINCT result;

-- Store the final output
STORE result INTO 'shared_folder/output' USING PigStorage(',');
