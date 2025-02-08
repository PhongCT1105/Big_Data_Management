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

   private static void deleteOutputIfExists(Configuration conf, String pathStr) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(pathStr);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text friendID = new Text();
        private final static IntWritable one = new IntWritable(1);
        private boolean isFirstLine = true;

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (isFirstLine) {
                isFirstLine = false;
                return;
            }
            if (fields.length >= 2) {
                friendID.set(fields[1].trim());
                context.write(friendID, one);
            }
        }
    }

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

    public static class FriendCountsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String pageID = parts[0].trim();
                String count = parts[1].trim();
                context.write(new Text(pageID), new Text("F:" + count));
            }
        }
    }
    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
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

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();

        deleteOutputIfExists(conf, "friend_counts");
        deleteOutputIfExists(conf, args[1]);

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

        Job job2 = Job.getInstance(conf, "Debug: Join with Page Owners (Reduce-Side Join)");
        job2.setJarByClass(TaskD.class);
        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job2, new Path("friend_counts"), TextInputFormat.class, FriendCountsMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, PagesMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[1])); // Final output directory

        if (!job2.waitForCompletion(true)) {
            throw new RuntimeException("Job 2 (Join with Page Owners) failed");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Delete existing output directories
        deleteOutputIfExists(conf, "friend_counts");
        deleteOutputIfExists(conf, args[1]);

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
