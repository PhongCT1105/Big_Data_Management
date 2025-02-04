package tasks;

import java.io.IOException;

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
            String line = value.toString();
            String[] fields = line.split(",");

            // Ignore the header row
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

    // Reducer Class
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    }

    // Debug Method
}
