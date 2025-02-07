package tasks;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Determine which people have favorites. That is, for each Facebook page owner,
// determine how many total accesses to Facebook pages they have made (as reported in
// the AccessLog) and how many distinct Facebook pages they have accessed in total.

public class TaskE {

    // Mapper Class
    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        private Text person = new Text();
        private Text page = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (key.toString().equals("0"))
                return;

            String personID = fields[1].trim();
            String pageID = "1," + fields[2].trim(); // Leading one to count pages

            person.set(personID);
            page.set(pageID);
            context.write(person, page);
        }
    }

    // Reducer Class
    public static class AccessLogReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccesses = 0;
            HashSet<String> distinctPages = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalAccesses += Integer.parseInt(parts[0]);

                for (int i = 1; i < parts.length; i++) {
                    distinctPages.add(parts[i]);
                }
            }
            context.write(key, new Text(totalAccesses + "," + distinctPages.size()));
        }
    }

    // Combiner Class
    public static class AccessLogCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccesses = 0;
            HashSet<String> distinctPages = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalAccesses += Integer.parseInt(parts[0]);

                for (int i = 1; i < parts.length; i++) {
                    distinctPages.add(parts[i]);
                }
            }
            String pages = ","; // Start with comma for easy concatenation
            for (String page : distinctPages) {
                pages += page + ",";
            }
            context.write(key, new Text(totalAccesses + pages));
        }
    }

    // Map-Reduce Method
    public void basic(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "access count");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Map-Reduce Combiner Method
    public void optimized(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "access count");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setCombinerClass(AccessLogCombiner.class); // Added combiner
        job.setReducerClass(AccessLogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Optimized solution for HDFS
    public static void main(String[] args) throws Exception {
        String csv_path = args[1] + "/access_logs.csv";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "access count");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setCombinerClass(AccessLogCombiner.class); // Added combiner
        job.setReducerClass(AccessLogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(csv_path));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
