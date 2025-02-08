package tasks;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GOptimized {
    // Mapper to create a simpler input for a second job
    public static class ActiveUserMapper extends Mapper<Object, Text, Text, Text> {
        private SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private long cutOffTime;

        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                String currentTimeString = context.getConfiguration().get("currentDate");
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date currentDate = inputFormat.parse(currentTimeString);
                cutOffTime = currentDate.getTime() - (14L * 24 * 60 * 60 * 1000); // 14 days in milliseconds
            } catch (ParseException e) {
                throw new RuntimeException("Error parsing current date", e);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            long latestAccess;
            String[] fields = value.toString().split(",");
            try {
                Date accessDate = inputFormat.parse(fields[4]);
                latestAccess = accessDate.getTime();
            } catch (ParseException e) {
                return;
            }

            if (latestAccess < cutOffTime) {
                context.write(new Text(fields[2]), new Text("active"));
            }

        }
    }

    // Reducer class to filter out active users
    public static class ActiveUsersReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    // Map-only job over DistributedCache
    public static class InactivityMapper extends Mapper<Object, Text, Text, Text> {
        private HashSet<String> activeUsers = new HashSet<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new FileNotFoundException("Required files missing in Distributed Cache");
            }

            // Active Users
            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;

            while ((line = reader.readLine()) != null) {
                activeUsers.add(line.trim());
            }

            reader.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String personID = fields[0].trim();
            String personName = fields[1].trim();

            if (!activeUsers.contains(personID)) {
                context.write(new Text(personID), new Text(personName));
            }
        }
    }

    // Map-side join without reducer
    public void optimized(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Set the current date in configuration
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        conf.set("currentDate", dateFormat.format(new Date()));

        // Temp directory for active users
        String tempDir = args[2] + "_temp";

        FileSystem fs = FileSystem.get(conf);

        // Cleanup old outputs if they exist
        fs.delete(new Path(tempDir), true);
        fs.delete(new Path(args[2]), true);

        Job job1 = Job.getInstance(conf, "active user tracker");
        job1.setJarByClass(TaskC.class);
        job1.setMapperClass(ActiveUserMapper.class);
        job1.setReducerClass(ActiveUsersReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(tempDir));

        System.out.println("Starting Job 1: Active User Tracker...");
        boolean activeUsersCompleted = job1.waitForCompletion(true);
        if (!activeUsersCompleted) {
            System.err.println("Active Users Extraction Failed.");
            System.exit(1);
        }
        System.out.println("Job 1 Completed Successfully.");

        Job job2 = Job.getInstance(conf, "inactivity log");
        job2.setJarByClass(TaskC.class);
        job2.setMapperClass(InactivityMapper.class);
        job2.setNumReduceTasks(0); // No reducer needed
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.addCacheFile(new URI(tempDir + "/part-r-00000"));

        FileInputFormat.addInputPath(job2, new Path(args[1]));

        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.out.println("Starting Job 2: Inactivity Mapper...");
        if (!job2.waitForCompletion(true)) {
            System.err.println("Job 2 Failed: Inactivity Mapper.");
            System.exit(1);
        }
        System.out.println("Job 2 Completed Successfully.");
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Set the current date in configuration
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        conf.set("currentDate", dateFormat.format(new Date()));

        // Temp directory for active users
        String tempDir = args[3] + "_temp";

        FileSystem fs = FileSystem.get(conf);

        // Cleanup old outputs if they exist
        fs.delete(new Path(tempDir), true);
        fs.delete(new Path(args[2]), true);

        Job job1 = Job.getInstance(conf, "active user tracker");
        job1.setJarByClass(TaskC.class);
        job1.setMapperClass(ActiveUserMapper.class);
        job1.setReducerClass(ActiveUsersReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(tempDir));

        System.out.println("Starting Job 1: Active User Tracker...");
        boolean activeUsersCompleted = job1.waitForCompletion(true);
        if (!activeUsersCompleted) {
            System.err.println("Active Users Extraction Failed.");
            System.exit(1);
        }
        System.out.println("Job 1 Completed Successfully.");

        Job job2 = Job.getInstance(conf, "inactivity log");
        job2.setJarByClass(TaskC.class);
        job2.setMapperClass(InactivityMapper.class);
        job2.setNumReduceTasks(0); // No reducer needed
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.addCacheFile(new URI(tempDir + "/part-r-00000"));

        FileInputFormat.addInputPath(job2, new Path(args[2]));

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.out.println("Starting Job 2: Inactivity Mapper...");
        if (!job2.waitForCompletion(true)) {
            System.err.println("Job 2 Failed: Inactivity Mapper.");
            System.exit(1);
        }
        System.out.println("Job 2 Completed Successfully.");
    }
}
