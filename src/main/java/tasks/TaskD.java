package tasks;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskD {

    // Utility function to delete existing output directories before running the job
    private static void deleteOutputIfExists(Configuration conf, String pathStr) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(pathStr);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Delete directory recursively
        }
    }

    // ------------------------------------------------------------------
    // JOB 1: Count Friend Mentions from friends.csv
    // ------------------------------------------------------------------

    // Mapper: Extract (p2, 1) from friends.csv
    public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text friendID = new Text();
        private final static IntWritable one = new IntWritable(1);
        private boolean isFirstLine = true;

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip header row if present
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }
            if (fields.length >= 2) { // Ensure at least two columns exist (p1, p2)
                friendID.set(fields[1].trim()); // p2 (friend)
                context.write(friendID, one);    // Emit (p2, 1)
            }
        }
    }

    // Reducer: Sum all friend counts for each p2
    public static class FriendsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // ------------------------------------------------------------------
    // JOB 2: Reduce‑Side Join of Friend Counts with Pages (Scalable)
    // ------------------------------------------------------------------

    /**
     * Mapper for the friend counts output (from Job 1).
     * Input lines are of the form: p2 [tab] count
     * It tags its output with "F:" to indicate a friend count record.
     */
    public static class FriendCountsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Expected format: p2 \t count
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String pageID = parts[0].trim();
                String count = parts[1].trim();
                context.write(new Text(pageID), new Text("F:" + count));
            }
        }
    }

    /**
     * Mapper for the pages file (pages.csv).
     * Expected input format: pageID,Name,Nationality (or at least pageID,Name)
     * It tags its output with "P:" to indicate a pages record.
     * This updated mapper now skips any header row that starts with "PersonID".
     */
    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // Skip the header row if it starts with "PersonID"
            if (line.toLowerCase().startsWith("personid")) {
                return;
            }
            String[] fields = line.split(",");
            if (fields.length >= 2) {
                String pageID = fields[0].trim();
                String ownerName = fields[1].trim();
                context.write(new Text(pageID), new Text("P:" + ownerName));
            }
        }
    }

    /**
     * Reducer: Joins friend counts with pages information.
     * For each page owner (p2), it outputs: p2 [tab] OwnerName,friendCount.
     * If a page owner does not appear in the friend counts, friendCount defaults to 0.
     */
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String friendCount = "0";  // Default friend count is 0
            String ownerName = "Unknown";
            for (Text val : values) {
                String record = val.toString();
                if (record.startsWith("F:")) {
                    friendCount = record.substring(2);
                } else if (record.startsWith("P:")) {
                    ownerName = record.substring(2);
                }
            }
            context.write(key, new Text(ownerName + "," + friendCount));
        }
    }

    // ------------------------------------------------------------------
    // Debug Method for Testing the Entire Flow (Job1 + Job2)
    // ------------------------------------------------------------------
    /**
     * The debug method runs both MapReduce jobs.
     *
     * args[0] = path to friends.csv (for Job 1)
     * args[1] = final output directory for the join job (Job 2)
     * args[2] = path to pages.csv (for Job 2)
     */
    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Delete existing output directories
        deleteOutputIfExists(conf, "friend_counts");
        deleteOutputIfExists(conf, args[1]);

        // -----------------------
        // JOB 1: Count Friend References
        // -----------------------
        Job job1 = Job.getInstance(conf, "Debug: Count Friend References");
        job1.setJarByClass(TaskD.class);
        job1.setMapperClass(FriendsMapper.class);
        job1.setReducerClass(FriendsReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[0]));      // Input: friends.csv
        FileOutputFormat.setOutputPath(job1, new Path("friend_counts")); // Intermediate output

        if (!job1.waitForCompletion(true)) {
            throw new RuntimeException("Job 1 (Count Friend References) failed");
        }

        // -----------------------
        // JOB 2: Reduce‑Side Join with Pages (Scalable)
        // -----------------------
        Job job2 = Job.getInstance(conf, "Debug: Join with Page Owners (Reduce-Side Join)");
        job2.setJarByClass(TaskD.class);
        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        // Use MultipleInputs to add both datasets:
        // Input 1: friend_counts (from Job 1)
        MultipleInputs.addInputPath(job2, new Path("friend_counts"), TextInputFormat.class, FriendCountsMapper.class);
        // Input 2: pages.csv (which might be large)
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, PagesMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[1])); // Final output directory

        if (!job2.waitForCompletion(true)) {
            throw new RuntimeException("Job 2 (Join with Page Owners) failed");
        }
    }

    // ------------------------------------------------------------------
    // Main Method (for Command-Line Execution)
    // ------------------------------------------------------------------
    /**
     * Command-line arguments:
     * args[0] = path to friends.csv (for Job 1)
     * args[1] = final output directory for the join job (Job 2)
     * args[2] = path to pages.csv (for Job 2)
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Delete existing output directories
        deleteOutputIfExists(conf, "friend_counts");
        deleteOutputIfExists(conf, args[1]);

        // -----------------------
        // JOB 1: Count Friend References
        // -----------------------
        Job job1 = Job.getInstance(conf, "Count Friend References");
        job1.setJarByClass(TaskD.class);
        job1.setMapperClass(FriendsMapper.class);
        job1.setReducerClass(FriendsReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[0]));      // Input: friends.csv
        FileOutputFormat.setOutputPath(job1, new Path("friend_counts")); // Intermediate output

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // -----------------------
        // JOB 2: Reduce‑Side Join with Pages (Scalable)
        // -----------------------
        Job job2 = Job.getInstance(conf, "Join with Page Owners (Reduce-Side Join)");
        job2.setJarByClass(TaskD.class);
        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job2, new Path("friend_counts"), TextInputFormat.class, FriendCountsMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, PagesMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[1])); // Final output directory

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
