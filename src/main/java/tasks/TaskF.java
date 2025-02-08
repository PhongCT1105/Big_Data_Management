package tasks;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TaskF {

    /**
     * Mapper for the friendship file (friends.csv).
     * Each input line is assumed to be a CSV record:
     * FriendRel,PersonID,MyFriend,DateOfFriendship,Desc
     * We skip the header and then use:
     *   p1 = PersonID (column index 1)
     *   p2 = MyFriend  (column index 2)
     */
    public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private boolean isHeader = true;  // To skip the header line

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header (assume header begins with "FriendRel")
            if (isHeader && line.startsWith("FriendRel")) {
                isHeader = false;
                return;
            }
            // Optionally, set isHeader to false after first record (for splits where header may appear only once)
            isHeader = false;

            // Split the CSV line on commas.
            String[] tokens = line.split(",");
            // Check if we have at least 3 columns (FriendRel,PersonID,MyFriend,...)
            if (tokens.length >= 3) {
                String p1 = tokens[1].trim();
                String p2 = tokens[2].trim();
                outKey.set(p1);
                // Tag this record as a friend relationship using prefix "F:"
                outValue.set("F:" + p2);
                context.write(outKey, outValue);
            }
        }
    }

    /**
     * Mapper for the page-access log (access_logs.csv).
     * Each input line is assumed to be a CSV record:
     * AccessID,ByWho,WhatPage,TypeOfAccess,AccessTime
     * We skip the header and then use:
     *   p1 = ByWho (column index 1)
     *   p2 = WhatPage (column index 2)
     */
    public static class VisitMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private boolean isHeader = true;  // To skip the header line

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header (assume header begins with "AccessID")
            if (isHeader && line.startsWith("AccessID")) {
                isHeader = false;
                return;
            }
            isHeader = false;

            // Split the CSV line on commas.
            String[] tokens = line.split(",");
            // Check if we have at least 3 columns.
            if (tokens.length >= 3) {
                String p1 = tokens[1].trim();
                String p2 = tokens[2].trim();
                outKey.set(p1);
                // Tag this record as a page visit using prefix "V:"
                outValue.set("V:" + p2);
                context.write(outKey, outValue);
            }
        }
    }

    /**
     * Reducer:
     * For each person p1, we collect all friend relationships and all visits.
     * If p1 has declared one or more friends and none of these friends appear
     * in the visit records, then p1 is considered “neglectful” and we output p1’s
     * PersonID and Name.
     *
     * The person details file (pages.csv) is loaded via the Distributed Cache.
     * Each record in pages.csv is a CSV:
     * PersonID,Name,Nationality,Country Code,Hobby
     * We skip the header and then use:
     *   personId = PersonID (column index 0)
     *   name = Name (column index 1)
     */
    public static class NeglectfulFriendsReducer extends Reducer<Text, Text, Text, Text> {
        // Map from PersonID to Name loaded from the Distributed Cache.
        private Map<String, String> personDetails = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load the person details file from the Distributed Cache.
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    Path path = new Path(cacheFile.getPath());
                    BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
                    String line;
                    boolean isHeader = true;
                    while ((line = reader.readLine()) != null) {
                        // Skip the header (assume header starts with "PersonID")
                        if (isHeader && line.startsWith("PersonID")) {
                            isHeader = false;
                            continue;
                        }
                        isHeader = false;
                        String[] tokens = line.split(",");
                        if (tokens.length >= 2) {
                            String personId = tokens[0].trim();
                            String name = tokens[1].trim();
                            personDetails.put(personId, name);
                        }
                    }
                    reader.close();
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // key is p1.
            Set<String> friends = new HashSet<>();
            Set<String> visits = new HashSet<>();

            // Process all records for this person p1.
            for (Text val : values) {
                String record = val.toString();
                if (record.startsWith("F:")) {
                    friends.add(record.substring(2));
                } else if (record.startsWith("V:")) {
                    visits.add(record.substring(2));
                }
            }

            // Condition: p1 has at least one declared friend and did not visit any of those friends.
            if (!friends.isEmpty()) {
                boolean visitedAnyFriend = false;
                for (String friend : friends) {
                    if (visits.contains(friend)) {
                        visitedAnyFriend = true;
                        break;
                    }
                }
                if (!visitedAnyFriend) {
                    String p1 = key.toString();
                    String name = personDetails.get(p1);
                    if (name == null) {
                        name = "Unknown";
                    }
                    context.write(new Text(p1), new Text(name));
                }
            }
        }
    }

    /**
     * The main method for production.
     * Expected arguments:
     *   args[0] = friendship file (friends.csv)
     *   args[1] = page-access log file (access_logs.csv)
     *   args[2] = person details file (pages.csv)
     *   args[3] = output path
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: TaskF <friendInput> <visitInput> <personDetails> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Neglectful Friends");
        job.setJarByClass(TaskF.class);

        // Add the person details file (pages.csv) to the Distributed Cache.
        job.addCacheFile(new Path(otherArgs[2]).toUri());

        // Set up the Reducer.
        job.setReducerClass(NeglectfulFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Use MultipleInputs to assign mappers.
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, FriendMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, VisitMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Debug function for testing purposes.
     * Expected arguments in this order:
     *   args[0] = friendship file (friends.csv)
     *   args[1] = page-access log file (access_logs.csv)
     *   args[2] = output directory
     *   args[3] = person details file (pages.csv)
     *
     * This method sets up and runs the job (similar to main) but does not call System.exit.
     */
    public void debug(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TaskF debug <friendInput> <visitInput> <output> <personDetails>");
            return;
        }
        // Reassign arguments to match our expected order.
        String friendInput   = args[0];
        String visitInput    = args[1];
        String output        = args[2];
        String personDetails = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Neglectful Friends Debug");
        job.setJarByClass(TaskF.class);

        // Add the person details file (pages.csv) to the Distributed Cache.
        job.addCacheFile(new Path(personDetails).toUri());

        // Set up the Reducer.
        job.setReducerClass(NeglectfulFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Use MultipleInputs to assign the mappers.
        MultipleInputs.addInputPath(job, new Path(friendInput), TextInputFormat.class, FriendMapper.class);
        MultipleInputs.addInputPath(job, new Path(visitInput), TextInputFormat.class, VisitMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(output));

        // Run the job and print the outcome.
        boolean success = job.waitForCompletion(true);
        System.out.println("Debug Job " + (success ? "succeeded" : "failed"));
    }
}
