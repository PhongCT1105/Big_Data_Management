package tasks;

import java.io.IOException;
// import java.util.HashMap;
// import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Write job(s) that reports for each country, how many of its citizens have a Facebook page

public class TaskC {
    // Mapper Class
    public class CountryMapper extends Mapper<Object, Text, Text, IntWritable> {
        // private HashMap<Text, Integer> countryMap = new HashMap<>();
        // private boolean firstLine = true;
        // private Text country = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Ignore the header row
            if (key.toString().equals("0"))
                return;

            // Country
            if (fields.length >= 5) {
                String country = fields[2].trim(); // May need to be nationality (2), or country code (3)
                // country.set(countryString);
                context.write(new Text(country), new IntWritable(1));
            }
        }
    }

    // Reducer Class
    public class CountryCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // Sum the counts for the country key
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Output the country and its total count
            result.set(sum);
            context.write(key, result);
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "country code count");
        job.setJarByClass(TaskC.class); // Replace TaskA with your actual class name
        job.setMapperClass(CountryMapper.class); // Your mapper class
        job.setReducerClass(CountryCountReducer.class); // Your reducer class

        job.setOutputKeyClass(Text.class); // Key will be the country code as String
        job.setOutputValueClass(IntWritable.class); // Value will be the count of occurrences

        // **Set number of reducers to 0 to avoid using a reducer**
        // job.setNumReduceTasks(0);

        // Input and Output paths from command line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete and exit with appropriate status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}