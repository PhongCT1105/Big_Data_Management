import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class kMeans {

    // --- Mapper Class ---
    public static class kMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get centroids from the DistributedCache (first file in cache)
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
                // If the line contains more than two tokens, assume it is of the format:
                // newCentroid Old:oldCentroid, so remove the "Old:" portion.
                if (tokens.length > 2) {
                    String[] trimTokens = line.split("Old:");
                    String newCentroidStr = trimTokens[0].trim();
                    tokens = newCentroidStr.split(",");
                }
                double[] centroid = new double[tokens.length];
                for (int i = 0; i < tokens.length; i++) {
                    centroid[i] = Double.parseDouble(tokens[i].trim());
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

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Optionally skip header line (if key equals 0)
            if (key.get() == 0) {
                return;
            }
            String[] tokens = value.toString().split(",");
            double[] point = new double[tokens.length];
            for (int i = 0; i < tokens.length; i++) {
                point[i] = Double.parseDouble(tokens[i].trim());
            }
            int centroidIndex = findNearestCentroid(point);
            double[] centroid = centroids.get(centroidIndex);

            // Build a centroid key string (e.g., "2500.0,3000.0")
            StringBuilder centroidKey = new StringBuilder();
            for (int i = 0; i < centroid.length; i++) {
                centroidKey.append(centroid[i]);
                if (i != centroid.length - 1) {
                    centroidKey.append(",");
                }
            }
            // Emit key = centroid; value = original point (as text)
            context.write(new Text(centroidKey.toString()), value);
        }
    }

    // --- Reducer Class for Intermediate Iterations ---
    public static class kMeansReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalXSum = 0;
            long totalYSum = 0;
            long totalCount = 0;
            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                if (tokens.length == 3) {
                    int partialX = Integer.parseInt(tokens[0].trim());
                    int partialY = Integer.parseInt(tokens[1].trim());
                    int partialCount = Integer.parseInt(tokens[2].trim());
                    totalXSum += partialX;
                    totalYSum += partialY;
                    totalCount += partialCount;
                } else if (tokens.length == 2) {
                    int x = Integer.parseInt(tokens[0].trim());
                    int y = Integer.parseInt(tokens[1].trim());
                    totalXSum += x;
                    totalYSum += y;
                    totalCount++;
                }
            }
            int newX = Math.round((float) totalXSum / totalCount);
            int newY = Math.round((float) totalYSum / totalCount);
            String newCentroid = newX + "," + newY;
            context.write(new Text(newCentroid + " Old:" + key.toString()), NullWritable.get());
        }
    }


    // --- Combiner Class for Optimization ---
    public static class kMeansCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int xSum = 0;
            int ySum = 0;
            int count = 0;
            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                int x = Integer.parseInt(tokens[0].trim());
                int y = Integer.parseInt(tokens[1].trim());
                xSum += x;
                ySum += y;
                count++;
            }
            // Emit combined sums and count as a single string "xSum,ySum,count"
            context.write(key, new Text(xSum + "," + ySum + "," + count));
        }
    }

    // --- Final Reducer Class using MultipleOutputs ---
    // This reducer writes two output types:
    // (a) Only cluster centers along with convergence flag.
    // (b) Each data point with its final cluster center assignment.
    public static class FinalReducer extends Reducer<Text, Text, Text, NullWritable> {
        private MultipleOutputs<Text, NullWritable> mos;
        private boolean converged;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<>(context);
            converged = context.getConfiguration().getBoolean("convergence", false);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalXSum = 0;
            long totalYSum = 0;
            long totalCount = 0;
            List<String> points = new ArrayList<>();

            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                // If the record is aggregated (from combiner), it should have three tokens.
                if (tokens.length == 3) {
                    int partialX = Integer.parseInt(tokens[0].trim());
                    int partialY = Integer.parseInt(tokens[1].trim());
                    int partialCount = Integer.parseInt(tokens[2].trim());
                    totalXSum += partialX;
                    totalYSum += partialY;
                    totalCount += partialCount;
                } else if (tokens.length == 2) {  // Otherwise, treat it as a single point.
                    int x = Integer.parseInt(tokens[0].trim());
                    int y = Integer.parseInt(tokens[1].trim());
                    totalXSum += x;
                    totalYSum += y;
                    totalCount++;
                }
                points.add(value.toString());
            }

            int newX = Math.round((float) totalXSum / totalCount);
            int newY = Math.round((float) totalYSum / totalCount);
            String newCentroid = newX + "," + newY;

            // Variation (a): Write final cluster center with convergence flag.
            String centersOutput = newCentroid + " Converged:" + (converged ? "yes" : "no");
            mos.write("centers", new Text(centersOutput), NullWritable.get());

            // Variation (b): Write each data point (or aggregated record) with its assigned final cluster center.
            for (String point : points) {
                String clusteredOutput = point + " assignedTo " + newCentroid;
                mos.write("clustered", new Text(clusteredOutput), NullWritable.get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }


    // --- Main Driver ---
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: kMeans <input> <output> <seeds> <K> <R> <checkConvergence>");
            System.exit(1);
        }
        Path input = new Path(args[0]);         // Data points input file
        Path output = new Path(args[1]);        // Base output directory
        Path seeds = new Path(args[2]);         // Seeds file (if used)
        int K = Integer.parseInt(args[3]);      // Number of clusters
        int R = Integer.parseInt(args[4]);      // Maximum number of iterations
        boolean checkConvergence = Boolean.parseBoolean(args[5]); // Whether to check for convergence

        double tolerance = 50.0;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path centroidPath = new Path("centroids.txt");

        // Generate initial centroids (randomly in this example)
        generateRandomCentroids(K, centroidPath, conf);

        // Loop through iterations
        for (int iteration = 0; iteration < R; iteration++) {
            Job job = Job.getInstance(conf, "K-Means Iteration " + iteration);
            job.setJarByClass(kMeans.class);

            System.out.println("Starting iteration " + iteration + " of " + R);
            System.out.println("Using centroid path: " + centroidPath.toString());

            job.setMapperClass(kMeansMapper.class);

            // ---------------------
            // 1) For non-final iterations, use combiner + kMeansReducer
            // 2) For final iteration, remove combiner and use FinalReducer
            if (iteration == R - 1) {
                // Final iteration
                job.setReducerClass(FinalReducer.class);

                // DO NOT set the combiner here:
                // job.setCombinerClass(kMeansCombiner.class);  // <--- omit this line!

                // Named outputs for final iteration
                MultipleOutputs.addNamedOutput(job, "centers", TextOutputFormat.class, Text.class, NullWritable.class);
                MultipleOutputs.addNamedOutput(job, "clustered", TextOutputFormat.class, Text.class, NullWritable.class);

                // Optionally, check convergence to set a flag
                boolean convergedFlag = hasConverged(fs, centroidPath, tolerance);
                job.getConfiguration().setBoolean("convergence", convergedFlag);

            } else {
                // Intermediate iteration
                job.setCombinerClass(kMeansCombiner.class);
                job.setReducerClass(kMeansReducer.class);
            }
            // ---------------------

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // Distribute current centroids file to mappers via DistributedCache
            job.addCacheFile(centroidPath.toUri());
            FileInputFormat.addInputPath(job, input);
            Path iterOutput = new Path(output + "/iter" + iteration);
            FileOutputFormat.setOutputPath(job, iterOutput);

            job.waitForCompletion(true);

            // For non-final iterations, update centroids and check for convergence
            if (iteration != R - 1) {
                Path newCentroids = new Path(iterOutput + "/part-r-00000");
                if (checkConvergence && hasConverged(fs, newCentroids, tolerance)) {
                    System.out.println("Centroids converged at iteration " + (iteration + 1));

                    // Run one final iteration (with FinalReducer, no combiner) for final output
                    iteration++;  // use next iteration number for final job
                    Job finalJob = Job.getInstance(conf, "K-Means Final Iteration " + iteration);
                    finalJob.setJarByClass(kMeans.class);

                    finalJob.setMapperClass(kMeansMapper.class);
                    // No combiner in final iteration
                    // finalJob.setCombinerClass(kMeansCombiner.class); // omit this line
                    finalJob.setReducerClass(FinalReducer.class);

                    MultipleOutputs.addNamedOutput(finalJob, "centers", TextOutputFormat.class, Text.class, NullWritable.class);
                    MultipleOutputs.addNamedOutput(finalJob, "clustered", TextOutputFormat.class, Text.class, NullWritable.class);

                    finalJob.setMapOutputKeyClass(Text.class);
                    finalJob.setMapOutputValueClass(Text.class);
                    finalJob.setOutputKeyClass(Text.class);
                    finalJob.setOutputValueClass(NullWritable.class);

                    finalJob.addCacheFile(newCentroids.toUri());
                    FileInputFormat.addInputPath(finalJob, input);
                    Path finalOutput = new Path(output + "/iter" + iteration);
                    FileOutputFormat.setOutputPath(finalJob, finalOutput);

                    finalJob.getConfiguration().setBoolean("convergence", true);

                    finalJob.waitForCompletion(true);
                    centroidPath = new Path(finalOutput + "/part-r-00000");
                    break;
                }
                centroidPath = newCentroids;
            }
        }
    }


    // --- Helper Method: Check Convergence ---
    // Reads a centroids file (with lines in the format "newCentroid Old:oldCentroid")
    // and determines if the Euclidean distance between old and new centroids is within tolerance.
    private static boolean hasConverged(FileSystem fs, Path centroidPath, double tolerance) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centroidPath)));
        String line;
        boolean converged = true;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(" Old:");
            if (parts.length < 2) continue; // Skip improperly formatted lines
            String[] newTokens = parts[0].split(",");
            double newX = Double.parseDouble(newTokens[0].trim());
            double newY = Double.parseDouble(newTokens[1].trim());
            String[] oldTokens = parts[1].split(",");
            double oldX = Double.parseDouble(oldTokens[0].trim());
            double oldY = Double.parseDouble(oldTokens[1].trim());
            double distance = Math.sqrt(Math.pow(newX - oldX, 2) + Math.pow(newY - oldY, 2));
            if (distance > tolerance) {
                converged = false;
                break;
            }
        }
        reader.close();
        return converged;
    }

    // --- Helper Method: Select Random Centroids from an Input File ---
    // (This method can be used if you prefer to choose initial centroids from your data.)
    private static void selectRandomCentroids(Path input, int K, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(input)));
        List<String> dataPoints = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            dataPoints.add(line);
        }
        reader.close();
        Collections.shuffle(dataPoints);
        List<String> initialCentroids = dataPoints.subList(0, K);
        Path centroidPath = new Path("centroids.txt");
        FSDataOutputStream outputStream = fs.create(centroidPath, true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        for (String centroid : initialCentroids) {
            writer.write(centroid);
            writer.newLine();
        }
        writer.close();
    }

    // --- Helper Method: Generate Random Centroids ---
    public static void generateRandomCentroids(int K, Path centroidPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(centroidPath, true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        Random random = new Random();
        for (int i = 0; i < K; i++) {
            int x = random.nextInt(5001); // random x between 0 and 5000
            int y = random.nextInt(5001); // random y between 0 and 5000
            writer.write(x + "," + y);
            writer.newLine();
        }
        writer.close();
    }
}
