import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskA {

    // Mapper Class
    public static class NationalityMapper extends Mapper<Object, Text, Text, Text> {
        private Text name = new Text();
        private Text hobby = new Text();
        private final String targetNationality = "Thailand"; // Change this as needed

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Ignore the header row
            if (key.toString().equals("0"))
                return;

            if (fields.length >= 5) { // Ensure at least 5 columns exist
                String nationality = fields[2].trim(); // Nationality column
                if (nationality.equals(targetNationality)) { // Filter by nationality
                    name.set(fields[1]); // Name column
                    hobby.set(fields[4]); // Hobby column
                    context.write(name, hobby);
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality filter");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(NationalityMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // **Set number of reducers to 0 to avoid using a reducer**
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality filter");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(NationalityMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // **Set number of reducers to 0 to avoid using a reducer**
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}