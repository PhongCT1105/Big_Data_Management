import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import tasks.TaskA;
import tasks.TaskC;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // String filePath = "pages.csv"; // Path to your CSV file

        // // ✅ Parse CSV file
        // CSVParser.parseCSV(filePath);

        // // ✅ Print headers
        // System.out.println("Headers: " + String.join(" | ", CSVParser.getHeaders()));

        // // ✅ Print entire table
        // CSVParser.printTable();

        // // ✅ Access a specific row
        // System.out.println("Row 1: " + String.join(" | ", CSVParser.getRow(1)));

        // // ✅ Access a specific value using column name
        // System.out.println("First row, Name: " + CSVParser.getValue(0, "PersonID"));
        // System.out.println("Second row, Department: " + CSVParser.getValue(1,
        // "Department"));

        // Task A job
        // Configuration confA = new Configuration();
        // Job jobA = Job.getInstance(confA, "nationality filter");
        // jobA.setJarByClass(TaskA.class);
        // jobA.setMapperClass(TaskA.NationalityMapper.class);
        // jobA.setOutputKeyClass(Text.class);
        // jobA.setOutputValueClass(Text.class);

        // // **Set number of reducers to 0 to avoid using a reducer**
        // jobA.setNumReduceTasks(0);

        // FileInputFormat.addInputPath(jobA, new Path(args[1]));
        // FileOutputFormat.setOutputPath(jobA, new Path(args[2]));

        // System.exit(jobA.waitForCompletion(true) ? 0 : 1);

        // Task C job
        Configuration confC = new Configuration();
        Job jobC = Job.getInstance(confC, "country citizens count");
        jobC.setJarByClass(TaskC.class);
        jobC.setMapperClass(TaskC.CountryMapper.class);
        // job.setCombinerClass(CountryCountReducer.class);
        jobC.setReducerClass(TaskC.CountryCountReducer.class);
        jobC.setOutputKeyClass(Text.class);
        jobC.setOutputValueClass(IntWritable.class);

        String output_path = "/Users/antoski/WPI/CS4433/Big_Data_Management/C_output";

        FileSystem fs = FileSystem.get(confC);
        Path outputPath = new Path(output_path);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Deleted existing output directory");
        }

        FileInputFormat.addInputPath(jobC, new Path("/Users/antoski/WPI/CS4433/Big_Data_Management/pages.csv"));
        FileOutputFormat.setOutputPath(jobC, outputPath);
        // Local mode, run the job locally
        if (jobC.waitForCompletion(true)) {
            System.out.println("Job completed successfully!");
        } else {
            System.out.println("Job failed!");
        }
    }
}
