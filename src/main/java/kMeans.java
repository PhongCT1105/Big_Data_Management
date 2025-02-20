import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No centroids file found in DistributedCache!");
            }

            Path centroidsPath = new Path(cacheFiles[0]);

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
            }
            reader.close();
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
        }
    }

    // Reducer Class
    public static class kMeansReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();
            int dimension = 0;

            for (Text val : values) {
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

            for (double[] point : points) {
                for (int i = 0; i < dimension; i++) {
                    newCentroid[i] += point[i];
                }
            }
            for (int i = 0; i < dimension; i++) {
                newCentroid[i] = Math.round(newCentroid[i] / points.size()); // Rounds to nearest whole number
            }

            // Output the new centroid
            StringBuilder sb = new StringBuilder();
            for (double v : newCentroid) {
                sb.append(v).append(",");
            }
            sb.setLength(sb.length() - 1); // Remove trailing comma
            System.out.println("Updated Centroid " + key.toString() + " -> " + sb.toString());

            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Path input = new Path(args[0]); // Path to data points
        Path output = new Path(args[1]); // Output path
        Path seeds = new Path(args[2]);
        Integer K = Integer.valueOf(args[3]);
        Integer R = Integer.valueOf(args[4]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path centroidPath = new Path("centroids.txt");
        // System.out.println("Centroid path: " + centroidPath.toString());

        // Step 1: Randomly select initial centroids
        generateRandomCentroids(K, centroidPath, conf);
        // Path centroids = new Path("centroids.txt");

        for (int iteration = 0; iteration < R; iteration++) {
            Job job = Job.getInstance(conf, "K-Means Iteration " + iteration);
            System.out.println("Starting iteration " + iteration + " of " + R);
            System.out.println("Centroid path: " + centroidPath.toString());
            job.setJarByClass(kMeans.class);
            job.setMapperClass(kMeansMapper.class);
            job.setReducerClass(kMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // Pass centroids via DistributedCache
            // conf.set("centroids", centroidPath.toString());
            job.addCacheFile(centroidPath.toUri());

            FileInputFormat.addInputPath(job, input);
            Path iterOutput = new Path(output + "/iter" + iteration);
            FileOutputFormat.setOutputPath(job, iterOutput);

            job.waitForCompletion(true);

            // here is where we would check if a tolerance passed/not.

            Path newCentroids = new Path(iterOutput + "/part-r-00000");

            centroidPath = newCentroids; // Move to the next iteration
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

    public static void generateRandomCentroids(int K, Path centroidPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(centroidPath, true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        Random random = new Random();

        for (int i = 0; i < K; i++) {
            int x = random.nextInt(5001); // Random integer x in range [0, 5000]
            int y = random.nextInt(5001); // Random integer y in range [0, 5000]
            writer.write(x + "," + y + "\n");
        }

        writer.close();
        System.out.println("Generated " + K + " random centroids in " + centroidPath.toString());
    }
}
