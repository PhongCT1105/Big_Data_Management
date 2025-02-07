package tasks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Report all Facebook users (name, and hobby) whose Nationality is the same as your
// own Nationality (pick one, e.g., “American” or “Indian”, etc.).

public class TaskA {

    // Mapper Class
    public static class NationalityMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Ignore the header row
            if (key.toString().equals("0"))
                return;

            String name = fields[1].trim();
            String hobby = fields[4].trim();
            String nationality = fields[2].trim();

            context.write(new Text(nationality), new Text(name + "," + hobby));
        }
    }

    // Reducer Class
    public static class NationalityReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                if (key.toString().equals("Thailand")) { // Change this as needed
                    String[] fields = val.toString().split(",");
                    context.write(new Text(fields[0]), new Text(fields[1]));
                }
            }
        }
    }

    // Map-Only Class
    public static class NationalityMapOnly extends Mapper<Object, Text, Text, Text> {
        private Text name = new Text();
        private Text hobby = new Text();
        private final String targetNationality = "Madagascar"; // Change this as needed

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Ignore the header row
            if (key.toString().equals("0"))
                return;

            String nationality = fields[2].trim();
            if (nationality.equals(targetNationality)) {
                name.set(fields[1]);
                hobby.set(fields[4]);
                context.write(name, hobby);
            }
        }
    }

    // Map-Reduce job
    public static void basic(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality filter");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(NationalityMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(NationalityReduce.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Optimized: Map-only job
    public static void maponly(String[] args) throws Exception {
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

    // Optimized solution for running over HDFS
    public static void main(String[] args) throws Exception {
        String csv_path = args[1] + "/pages.csv";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality filter");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(NationalityMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // **Set number of reducers to 0 to avoid using a reducer**
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(csv_path));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}