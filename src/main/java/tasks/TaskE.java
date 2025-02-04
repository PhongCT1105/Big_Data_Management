package tasks;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.IntWritable;
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
    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text person = new Text();
        private Text page = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (key.toString().equals("0"))
                return;

            if (fields.length >= 5) { // Ensure at least 5 columns exist
                String personID = fields[1].trim(); // PersonID column
                String pageID = fields[2].trim(); // PageID column

                person.set(personID);
                page.set(pageID);
                context.write(person, page);
            }
        }
    }

    // Combiner Class
    // Goal would be to combine the values for each key
    public static class AccessReducer extends Reducer<Text, Text, Text, Text> {
        // private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccesses = 0;
            Set<String> distinctPages = new HashSet<>();

            for (Text val : values) {
                totalAccesses++;
                distinctPages.add(val.toString());
            }

            String result = totalAccesses + "," + distinctPages.size();
            context.write(key, new Text(result));
        }
    }

    // Reducer Class
    // Goal would be to return the total number of accesses and the number of
    // distinct pages
    // public static class IntSumReducer extends Reducer<Text, IntWritable, Text,
    // IntWritable> {
    // private IntWritable result = new IntWritable();

    // public void reduce(Text key, Iterable<IntWritable> values, Context context)
    // throws IOException, InterruptedException {
    // int sum = 0;
    // for (IntWritable val : values) {
    // sum += val.get();
    // }
    // result.set(sum);
    // context.write(key, result);
    // }
    // }

    // Debug Method
    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "access count");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(AccessMapper.class);
        // job.setCombinerClass(AccessCombiner.class);
        job.setReducerClass(AccessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
