package tasks;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class TaskH {

    /////////////////////////////////////////////////////////////
    // Job 1: Count friend relationships (popularity measure)
    /////////////////////////////////////////////////////////////

    /**
     * Mapper for friends.csv.
     * Each record is a CSV line:
     * FriendRel,PersonID,MyFriend,DateOfFriendship,Desc
     * We skip the header and emit (MyFriend, 1)
     */
    public static class FriendCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private boolean isHeader = true;
        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // Skip header (assume header starts with "FriendRel")
            if (isHeader && line.startsWith("FriendRel")) {
                isHeader = false;
                return;
            }
            isHeader = false;
            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                // Use the MyFriend field (column index 2)
                String person = tokens[2].trim();
                outKey.set(person);
                context.write(outKey, one);
            }
        }
    }

    /**
     * Reducer for friend counts.
     * Sums the counts for each person and also updates a global counter.
     */
    public static class FriendCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Update global counter for total friend relationships
            context.getCounter("FriendStats", "TotalFriends").increment(sum);
            context.write(key, new IntWritable(sum));
        }
    }

    /////////////////////////////////////////////////////////////
    // Job 2: Join friend counts with pages.csv and filter by average
    /////////////////////////////////////////////////////////////

    /**
     * Mapper for the friend count output from Job 1.
     * Each record is: PersonID<TAB>Count
     * We tag these records with prefix "C:".
     */
    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length == 2) {
                String person = tokens[0].trim();
                String count = tokens[1].trim();
                context.write(new Text(person), new Text("C:" + count));
            }
        }
    }

    /**
     * Mapper for pages.csv.
     * Each record is a CSV line:
     * PersonID,Name,Nationality,Country Code,Hobby
     * We skip the header and emit (PersonID, "P:" + Name)
     */
    public static class PagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // Skip header (assume header starts with "PersonID")
            if (isHeader && line.startsWith("PersonID")) {
                isHeader = false;
                return;
            }
            isHeader = false;
            String[] tokens = line.split(",");
            if (tokens.length >= 2) {
                String person = tokens[0].trim();
                String name = tokens[1].trim();
                context.write(new Text(person), new Text("P:" + name));
            }
        }
    }

    /**
     * Reducer for joining friend count and pages records.
     * It retrieves the global average from configuration.
     * For each person, if the friend count is greater than the average,
     * output the person's details (PersonID, Name, and count).
     * If a person does not appear in the friend count file, assume count = 0.
     */
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private double avg;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            avg = context.getConfiguration().getDouble("global.average", 0.0);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            String name = null;
            // There may be one or two kinds of tagged values.
            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("C:")) {
                    count = Integer.parseInt(s.substring(2));
                } else if (s.startsWith("P:")) {
                    name = s.substring(2);
                }
            }
            // If a person never appears in the count file, count remains 0.
            if (count > avg && name != null) {
                context.write(key, new Text(name + "\t" + count));
            }
        }
    }

    /////////////////////////////////////////////////////////////
    // Main method: chaining two jobs
    /////////////////////////////////////////////////////////////

    /**
     * Main function for production.
     *
     * Expected arguments:
     *   args[0] = friends.csv (friend relationships file)
     *   args[1] = pages.csv (people details file)
     *   args[2] = intermediate output path (for Job 1)
     *   args[3] = final output path (for Job 2)
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: TaskH <friends.csv> <pages.csv> <intermediate_output> <final_output>");
            System.exit(2);
        }

        // --------------------
        // Job 1: Friend Count
        // --------------------
        Job job1 = Job.getInstance(conf, "Friend Count");
        job1.setJarByClass(TaskH.class);
        job1.setMapperClass(FriendCountMapper.class);
        job1.setReducerClass(FriendCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }

        // Get total friend relationships from Job 1 counters.
        long totalFriends = job1.getCounters().findCounter("FriendStats", "TotalFriends").getValue();

        // --------------------
        // Count total persons from pages.csv.
        // --------------------
        long totalPersons = 0;
        Path pagesPath = new Path(otherArgs[1]);
        FileSystem fs = pagesPath.getFileSystem(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pagesPath)));
        String line;
        boolean headerSkipped = false;
        while ((line = br.readLine()) != null) {
            if (!headerSkipped) { headerSkipped = true; continue; }
            totalPersons++;
        }
        br.close();

        double avg = totalPersons == 0 ? 0.0 : (double) totalFriends / totalPersons;
        System.out.println("Total Friends = " + totalFriends + ", Total Persons = " + totalPersons + ", Average = " + avg);

        // --------------------
        // Job 2: Join and Filter by popularity
        // --------------------
        Configuration conf2 = new Configuration();
        conf2.setDouble("global.average", avg);

        Job job2 = Job.getInstance(conf2, "Popular People Join");
        job2.setJarByClass(TaskH.class);
        // Use MultipleInputs: one input is the output of Job 1 (friend counts)
        // and the other is pages.csv.
        MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, CountMapper.class);
        MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, PagesMapper.class);
        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

        success = job2.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }

    /////////////////////////////////////////////////////////////
    // Debug function (for testing) - similar to main but without System.exit.
    /////////////////////////////////////////////////////////////

    /**
     * Debug function for testing purposes.
     *
     * Expected arguments:
     *   args[0] = friends.csv
     *   args[1] = pages.csv
     *   args[2] = intermediate output path (for Job 1)
     *   args[3] = final output path (for Job 2)
     */
    public void debug(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TaskH debug <friends.csv> <pages.csv> <intermediate_output> <final_output>");
            return;
        }
        Configuration conf = new Configuration();
        // --------------------
        // Job 1: Friend Count
        // --------------------
        Job job1 = Job.getInstance(conf, "Friend Count Debug");
        job1.setJarByClass(TaskH.class);
        job1.setMapperClass(FriendCountMapper.class);
        job1.setReducerClass(FriendCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.err.println("Job 1 failed.");
            return;
        }

        long totalFriends = job1.getCounters().findCounter("FriendStats", "TotalFriends").getValue();

        // --------------------
        // Count total persons from pages.csv.
        // --------------------
        long totalPersons = 0;
        Path pagesPath = new Path(args[1]);
        FileSystem fs = pagesPath.getFileSystem(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pagesPath)));
        String line;
        boolean headerSkipped = false;
        while ((line = br.readLine()) != null) {
            if (!headerSkipped) { headerSkipped = true; continue; }
            totalPersons++;
        }
        br.close();

        double avg = totalPersons == 0 ? 0.0 : (double) totalFriends / totalPersons;
        System.out.println("Debug: Total Friends = " + totalFriends + ", Total Persons = " + totalPersons + ", Average = " + avg);

        // --------------------
        // Job 2: Join and Filter
        // --------------------
        Configuration conf2 = new Configuration();
        conf2.setDouble("global.average", avg);

        Job job2 = Job.getInstance(conf2, "Popular People Join Debug");
        job2.setJarByClass(TaskH.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, CountMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, PagesMapper.class);
        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        success = job2.waitForCompletion(true);
        System.out.println("Debug Job 2 " + (success ? "succeeded" : "failed"));
    }
}
