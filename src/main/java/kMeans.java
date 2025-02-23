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
    public static class kMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
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

                // If length > 2, we are past the 1st iteration
                if (tokens.length > 2) {
                    String[] trimTokens = line.split("Old:");
                    String newCentroid = trimTokens[0].trim();
                    tokens = newCentroid.split(",");
                }

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

            double[] centroid = centroids.get(centroidIndex);
            String centroidString = String.valueOf(centroid[0]) + "," + String.valueOf(centroid[1]);

            context.write(new Text(centroidString), value);
        }
    }

    // Reducer Class
    public static class kMeansReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int xSum = 0;
            int ySum = 0;
            int numPoints = 0;

            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                xSum += Integer.parseInt(tokens[0]); // Accumulate sum_x
                ySum += Integer.parseInt(tokens[1]); // Accumulate sum_y

                // Combiner
                numPoints += Integer.parseInt(tokens[2]); // Accumulate count

                // Without combiner
                // numPoints++;
            }

            Integer newX = Math.round(xSum / numPoints);
            Integer newY = Math.round(ySum / numPoints);
            String newCentroid = newX.toString() + "," + newY.toString();
            String output = newCentroid + " Old:" + key.toString();

            context.write(new Text(output), NullWritable.get());
        }
    }

    // Combiner Class
    public static class kMeansCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int xSum = 0;
            int ySum = 0;
            int numPoints = 0;

            for (Text val : values) {

                String[] tokens = val.toString().split(",");
                int x = Integer.parseInt(tokens[0]);
                int y = Integer.parseInt(tokens[1]);

                xSum += x;
                ySum += y;
                numPoints++;
            }

            context.write(key, new Text(xSum + "," + ySum + "," + numPoints));
        }
    }

    public static void main(String[] args) throws Exception {
        Path input = new Path(args[0]); // Path to data points
        Path output = new Path(args[1]); // Output path
        Path seeds = new Path(args[2]); // Random seeds
        Integer K = Integer.valueOf(args[3]); // Number of clusters
        Integer R = Integer.valueOf(args[4]); // Number of iterations
        boolean checkConvergence = Boolean.parseBoolean(args[5]); // True or False

        double tolerance = 50.0;

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path centroidPath = new Path("centroids.txt");

        // Randomly select initial centroids
        generateRandomCentroids(K, centroidPath, conf); // Replace with line below with new generated k_seeds (x & y
                                                        // between 0-5000)
        // selectRandomCentroids(seeds, K, conf);

        for (int iteration = 0; iteration < R; iteration++) {
            Job job = Job.getInstance(conf, "K-Means Iteration " + iteration);
            System.out.println("Starting iteration " + iteration + " of " + R);
            System.out.println("Centroid path: " + centroidPath.toString());
            job.setJarByClass(kMeans.class);
            job.setMapperClass(kMeansMapper.class);
            job.setCombinerClass(kMeansCombiner.class); // Combiner
            job.setReducerClass(kMeansReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // Pass centroids via DistributedCache
            job.addCacheFile(centroidPath.toUri());

            FileInputFormat.addInputPath(job, input);
            Path iterOutput = new Path(output + "/iter" + iteration);
            FileOutputFormat.setOutputPath(job, iterOutput);

            job.waitForCompletion(true);

            Path newCentroids = new Path(iterOutput + "/part-r-00000");

            if (checkConvergence && R > 1 && hasConverged(fs, newCentroids, tolerance)) {
                System.out.println("Centroids converged in " + (iteration + 1) + " iterations!");
                break;
            }

            centroidPath = newCentroids; // Move to the next iteration
        }
    }

    // **Helper Function: Check if centroids have converged**
    private static boolean hasConverged(FileSystem fs, Path centroidPath, double tolerance) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centroidPath)));

        String line;
        boolean hasConverged = true; // Assume convergence unless proven otherwise

        while ((line = reader.readLine()) != null) {
            // Extract "newX,newY" before "Old:"
            String[] parts = line.split(" Old:");
            String[] newTokens = parts[0].split(",");
            double newX = Double.parseDouble(newTokens[0]);
            double newY = Double.parseDouble(newTokens[1]);

            // Extract "oldX,oldY" after "Old:"
            String[] oldTokens = parts[1].split(",");
            double oldX = Double.parseDouble(oldTokens[0]);
            double oldY = Double.parseDouble(oldTokens[1]);

            // Compute Euclidean distance
            double distance = Math.sqrt(Math.pow(newX - oldX, 2) + Math.pow(newY - oldY, 2));

            // If any centroid moved more than the tolerance, stop checking
            if (distance > tolerance) {
                hasConverged = false;
                break;
            }
        }

        reader.close();
        return hasConverged;
    }

    private static void selectRandomCentroids(Path input, int K, Configuration conf) throws IOException {
        /**
         * This mehod will be reimplemented, just need to replace the k_seeds.csv file!
         */
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
    }
}
