package tasks;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

// Identify all "disconnected" people (and return their PersonID and Name) that have not
// accessed the Facebook site for 14 days or longer (i.e., meaning no entries in the
// AccessLog exist in the last 14 days).

public class TaskG {

    // Mapper Class for Access Logs
    public static class LogMapper extends Mapper<Object, Text, Text, Text> {
        private Text personID = new Text();
        private Text timeStamp = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (key.toString().equals("0"))
                return;

            if (fields.length >= 5) {
                String personIDString = fields[1].trim();
                // String personNameString = fields[1];
                String accessTimeString = fields[4].trim();

                personID.set(personIDString);
                timeStamp.set("Log," + accessTimeString);
                context.write(personID, timeStamp);
            }
        }
    }

    // Mapper class for Pages
    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        private Text personID = new Text();
        private Text personName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (key.toString().equals("0"))
                return;

            if (fields.length >= 5) {
                String personIDString = fields[0].trim();
                String personNameString = fields[1].trim();

                personID.set(personIDString);
                personName.set("Name," + personNameString);
                context.write(personID, personName);
            }
        }
    }

    // Reducer Class to join the two mappers
    public static class InactivtyReducer extends Reducer<Text, Text, Text, Text> {
        private SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private long cutOffTime;

        public void setup(Context context) throws IOException, InterruptedException {
            String currentTimeString = context.getConfiguration().get("currentDate");
            try {
                // Calculate the cutoff time (14 days ago)
                Date currentDate = inputFormat.parse(currentTimeString);
                cutOffTime = currentDate.getTime() - (14L * 24 * 60 * 60 * 1000); // 14 days in milliseconds
            } catch (ParseException e) {
                throw new RuntimeException("Error parsing current date", e);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String personName = null;
            long latestAccess = 0;

            // Iterate through the values
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("Name")) {
                    personName = parts[1];
                } else if (parts[0].equals("Log")) {
                    try {
                        Date accessDate = inputFormat.parse(parts[1]);
                        latestAccess = accessDate.getTime();
                    } catch (ParseException e) {
                        throw new RuntimeException("Error parsing access time", e);
                    }
                }
            }

            if (personName != null && latestAccess > cutOffTime) {
                context.write(key, new Text(personName));
            }
        }
    }

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

            if (latestAccess >= cutOffTime) {
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
                throw new FileNotFoundException("Active Users file not found in Distributed Cache");
            }

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
            if (!activeUsers.contains(fields[0])) {
                context.write(new Text(fields[0]), new Text(fields[1]));
            }
        }
    }

    // Reduce-side join with two mappers
    public void basic(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Set the current date in configuration
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        conf.set("currentDate", dateFormat.format(new Date()));

        System.out.println("Current Date: " + conf.get("currentDate"));

        Job job = Job.getInstance(conf, "inactivity log");
        job.setJarByClass(TaskC.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, LogMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageMapper.class);

        job.setReducerClass(InactivtyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Map-side join without reducer
    public void optimized(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Set the current date in configuration
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        conf.set("currentDate", dateFormat.format(new Date()));

        // Temp directory for active users
        String tempDir = args[2] + "/active";

        Job job1 = Job.getInstance(conf, "active user tracker");
        job1.setJarByClass(TaskC.class);
        job1.setMapperClass(ActiveUserMapper.class);
        job1.setReducerClass(ActiveUsersReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(tempDir));

        boolean activeUsersCompleted = job1.waitForCompletion(true);
        if (!activeUsersCompleted) {
            System.err.println("Active Users Extraction Failed.");
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "inactivity log");
        job2.setJarByClass(TaskC.class);
        job2.setMapperClass(InactivityMapper.class);
        job2.setNumReduceTasks(0); // No reducer needed
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.addCacheFile(new URI(tempDir + "/part-r-00000"));

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileInputFormat.addInputPath(job2, new Path(args[2]));
    }

    // Optimized solution for HDFS
    public static void main(String[] args) throws Exception {
        String log_csv = args[1] + "/access_logs.csv";
        String page_csv = args[1] + "/pages.csv";

        Configuration conf = new Configuration();

        // Set the current date in configuration
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        conf.set("currentDate", dateFormat.format(new Date()));

        System.out.println("Current Date: " + conf.get("currentDate"));

        Job job = Job.getInstance(conf, "inactivity log");
        job.setJarByClass(TaskC.class);

        MultipleInputs.addInputPath(job, new Path(log_csv), TextInputFormat.class, LogMapper.class);
        MultipleInputs.addInputPath(job, new Path(page_csv), TextInputFormat.class, PageMapper.class);

        job.setReducerClass(InactivtyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
