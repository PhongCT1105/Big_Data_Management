import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kMeans {

    // Mapper Class
    public static class kMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private List<double[]> centroids = new ArrayList<>();

        protected void setup(Context context) throws IOException {
            // Load centroids from the distributed cache
            // BufferedReader reader = new BufferedReader(new FileReader("centroids.txt"));
            Path centroidsPath = new Path("centroids.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                double[] centroid = new double[tokens.length];
                for (int i = 0; i < tokens.length; i++) {
                    centroid[i] = Double.parseDouble(tokens[i]);
                }
                centroids.add(centroid);
                System.out.println(centroid[0] + "," + centroid[1]);
            }
            reader.close();
            // System.out.println("Centroids read in");
        }

        private int findNearestCentroid(double[] point) {
            int closestIndex = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = euclideanDistance(point, centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    closestIndex = i;
                }
            }
            return closestIndex;
        }

        private double euclideanDistance(double[] p1, double[] p2) {
            double sum = 0.0;
            for (int i = 0; i < p1.length; i++) {
                sum += Math.pow(p1[i] - p2[i], 2);
            }
            return Math.sqrt(sum);
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            double[] point = new double[tokens.length];

            if (key.toString().equals("0"))
                return;

            for (int i = 0; i < tokens.length; i++) {
                point[i] = Double.parseDouble(tokens[i]);
            }

            int centroidIndex = findNearestCentroid(point);
            context.write(new IntWritable(centroidIndex), value);

            // Debugging
            // System.out.println("Emitting -> Centroid Index: " + centroidIndex + " |
            // Point: " + value.toString());
        }
    }

    // Reducer Class
    public static class kMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();
            int dimension = 0;
            // System.out.println("Reducer reached");

            for (Text val : values) {
                // System.out.println("Reducer received -> Centroid: " + key.get() + " | Point:
                // " + val.toString());

                String[] tokens = val.toString().split(",");
                double[] point = new double[tokens.length];
                // Get list of points
                for (int i = 0; i < tokens.length; i++) {
                    point[i] = Double.parseDouble(tokens[i]);
                }
                points.add(point);
                dimension = tokens.length;
            }

            double[] newCentroid = new double[dimension];

            // go through and
            for (double[] point : points) {
                for (int i = 0; i < dimension; i++) {
                    newCentroid[i] += point[i];
                }
            }
            for (int i = 0; i < dimension; i++) {
                newCentroid[i] /= points.size();
            }

            // Output the new centroid
            StringBuilder sb = new StringBuilder();
            for (double v : newCentroid) {
                sb.append(v).append(",");
            }
            sb.setLength(sb.length() - 1); // Remove trailing comma
            context.write(key, new Text(sb.toString()));
            // System.out.println("Reducer Output -> Centroid: " + key.get() + " | New
            // Centroid: " + sb.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]); // Path to data points
        Path output = new Path(args[1]); // Output path
        Integer K = Integer.valueOf(args[2]);
        Integer endIteration = Integer.valueOf(args[3]);

        // Step 1: Randomly select initial centroids
        selectRandomCentroids(new Path("k_seeds.csv"), K, conf);
        Path centroids = new Path("centroids.txt");

        int iteration = 0;
        while (iteration < endIteration) {
            Job job = Job.getInstance(conf, "K-Means Iteration " + iteration);
            job.setJarByClass(kMeans.class);
            job.setMapperClass(kMeansMapper.class);
            job.setReducerClass(kMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);

            centroids = output;
            iteration++;
            System.out.println("Iteration " + iteration + " complete!");
        }
    }

    private static void selectRandomCentroids(Path input, int K, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(input)));

        List<String> dataPoints = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            dataPoints.add(line);
        }
        reader.close();

        // Shuffle and pick K random points
        Collections.shuffle(dataPoints);
        List<String> initialCentroids = dataPoints.subList(0, K);

        // Write to centroids.txt
        Path centroidPath = new Path("centroids.txt");
        FSDataOutputStream outputStream = fs.create(centroidPath, true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        for (String centroid : initialCentroids) {
            writer.write(centroid + "\n");
        }
        writer.close();
    }
}
