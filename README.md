# Big_Data_Management

## Running Tasks

### Locally

All tasks can be evaluated with the local data in Project1Test.

On line 9, there is a directory path `path` that will need to be modified based on where the project is saved.

### Over HDFS

All tasks have their own main to create JAR files.
The assumption is that all three data files `pages.csv, friends.csv, and access_logs.csv` are already loaded onto HDFS.
To run a single task over Haddop: `hadoop/bin/hadoop jar ~/shared_folder/TaskG.jar TaskG /user/cs4433/project1/input/ /user/cs4433/project1/output`

- Modify `TaskG.jar` to appropriate file name
- `/user/cs4433/project1/input`: this must be the location of the data files on HDFS, the file names are hardcoded.

### Contributors
