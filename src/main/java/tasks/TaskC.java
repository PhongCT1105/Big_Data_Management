package tasks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Write job(s) that reports for each country, how many of its citizens have a Facebook page

public class TaskC {

    // Mapper Class
    public static class CountryMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            String country = fields[2].trim();
            context.write(new Text(country), one);
        }
    }

    // Reducer Class
    public static class CountryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Combiner Class
    public static class CountryCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Map-Reduce job
    public void basic(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "country citizen counter");
        job.setJarByClass(TaskC.class);
        job.setMapperClass(CountryMapper.class);
        job.setReducerClass(CountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Map-Reduce combiner job
    public void optimized(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "country citizen counter");
        job.setJarByClass(TaskC.class);
        job.setMapperClass(CountryMapper.class);
        job.setCombinerClass(CountryCombiner.class); // Combiner
        job.setReducerClass(CountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Optimized solution for HDFS
    public static void main(String[] args) throws Exception {
        String csv_path = args[1] + "/pages.csv";
        String output_path = args[2] + "/C";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "country citizen counter");
        job.setJarByClass(TaskC.class);
        job.setMapperClass(CountryMapper.class);
        job.setCombinerClass(CountryCombiner.class); // Combiner
        job.setReducerClass(CountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(csv_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}