package tasks;

import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

    // Debug method
    public void debug(String[] args) throws Exception {
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

}
