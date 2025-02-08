package tasks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Report all Facebook users (name, and hobby) whose Nationality is the same as your
// own Nationality (pick one, e.g., “American” or “Indian”, etc.).

public class TaskA {

    // Map-Only Class
    public static class NationalityMapOnly extends Mapper<Object, Text, Text, Text> {
        private final String targetNationality = "France"; // Change this as needed

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields[2].trim().equalsIgnoreCase(targetNationality.trim())) {
                context.write(new Text(fields[1].trim()), new Text(fields[4].trim()));
            }
        }
    }

    // Map-only job
    public static void optimized(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality filter");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(NationalityMapOnly.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set number of reducers to 0 to avoid using a reducer
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Optimized solution for HDFS
    public static void main(String[] args) throws Exception {
        String csv_path = args[1] + "/pages.csv";
        String output_path = args[2] + "/A";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality filter");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(NationalityMapOnly.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(csv_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}