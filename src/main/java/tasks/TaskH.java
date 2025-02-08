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

    public static class FriendCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private boolean isHeader = true;
        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: TaskH <friends.csv> <pages.csv> <intermediate_output> <final_output>");
            System.exit(2);
        }

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

        long totalFriends = job1.getCounters().findCounter("FriendStats", "TotalFriends").getValue();

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

        Configuration conf2 = new Configuration();
        conf2.setDouble("global.average", avg);

        Job job2 = Job.getInstance(conf2, "Popular People Join");
        job2.setJarByClass(TaskH.class);
        MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, CountMapper.class);
        MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, PagesMapper.class);
        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

        success = job2.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
    public void debug(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TaskH debug <friends.csv> <pages.csv> <intermediate_output> <final_output>");
            return;
        }
        Configuration conf = new Configuration();

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
