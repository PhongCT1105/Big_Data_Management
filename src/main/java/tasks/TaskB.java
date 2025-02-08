package tasks;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskB {

    // ** Function to Delete Existing Output Directory **
    private static void deleteOutputIfExists(Configuration conf, String pathStr) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(pathStr);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Delete directory recursively
        }
    }

    // --- JOB 1: Extract Top 10 Pages from Access Logs ---

    // Mapper Class: Emits (PageID, 1) for each access log record
    public static class AccessLogMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text pageID = new Text();
        private final static IntWritable one = new IntWritable(1);
        private boolean isFirstLine = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip header line
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            // Expecting at least 3 fields with page id in the third field
            if (fields.length >= 3) {
                pageID.set(fields[2].trim());
                context.write(pageID, one);
            }
        }
    }

    // Reducer: Aggregates counts and maintains a fixed-size min‑heap to extract the top 10 pages
    public static class AccessLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Simple helper class to hold page id and count
        public static class PageCount {
            String pageID;
            int count;
            public PageCount(String id, int cnt) {
                this.pageID = id;
                this.count = cnt;
            }
        }
        // Min‑heap (priority queue) of fixed size 10 (smallest count at the top)
        private PriorityQueue<PageCount> topPagesHeap = new PriorityQueue<>(10, Comparator.comparingInt(pc -> pc.count));

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            PageCount pc = new PageCount(key.toString(), sum);
            if (topPagesHeap.size() < 10) {
                topPagesHeap.offer(pc);
            } else if (topPagesHeap.peek().count < sum) {
                topPagesHeap.poll();
                topPagesHeap.offer(pc);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Extract and sort the top 10 pages in descending order (largest count first)
            List<PageCount> topList = new ArrayList<>(topPagesHeap);
            topList.sort((a, b) -> Integer.compare(b.count, a.count));
            for (PageCount pc : topList) {
                context.write(new Text(pc.pageID), new IntWritable(pc.count));
            }
        }
    }

    // --- JOB 2: Map-Side Join Using Cached Top 10 Pages and Large Pages File ---

    /*
     * Updated Strategy for Job 2:
     * - Input: pages.csv (which is assumed to be large)
     * - Distributed Cache: the top10pages file (from Job 1 output, e.g., "top10pages/part-r-00000")
     * - For each record in pages.csv (format: pageID,Name,Nationality), the mapper checks if the pageID
     *   is among the top 10 (using the cached file). If yes, it emits the record.
     */

    public static class PageJoinMapper extends Mapper<Object, Text, Text, Text> {
        // HashSet to store top 10 page IDs (cached from the top10pages file)
        private HashSet<String> topPages = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load the top10pages file from the distributed cache.
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // Assume the first cached file is our top10pages file.
                Path topPagesCacheFile = cacheFiles[0];
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(topPagesCacheFile)));
                String line;
                while ((line = reader.readLine()) != null) {
                    // Expecting format: pageID \t count
                    String[] fields = line.split("\t");
                    if (fields.length == 2) {
                        topPages.add(fields[0]);
                    }
                }
                reader.close();
            }
        }

        // In the map method, the input is a record from pages.csv.
        // Expecting pages.csv format: pageID,Name,Nationality
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length >= 3) {
                String pageID = fields[0].trim();  // Page ID is in the first field
                // If this page is among the top 10, output its details (Name and Nationality)
                if (topPages.contains(pageID)) {
                    String pageData = fields[1].trim() + "," + fields[2].trim();
                    context.write(new Text(pageID), new Text(pageData));
                }
            }
        }
    }

    // Reducer to Remove Duplicates (if any)
    public static class UniqueReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
                break; // Emit only one record per page
            }
        }
    }

    // --- Main / Debug Methods ---

    // Debug method (if you want to run in a debugging context)
    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Delete existing output directories
        deleteOutputIfExists(conf, "top10pages");
        deleteOutputIfExists(conf, args[2]);

        // **Job 1: Extract Top 10 Pages from Access Logs**
        Job job1 = Job.getInstance(conf, "Debug: Extract Top 10 Pages");
        job1.setJarByClass(TaskB.class);
        job1.setMapperClass(AccessLogMapper.class);
        job1.setReducerClass(AccessLogReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(args[0])); // Input: access_logs.csv
        FileOutputFormat.setOutputPath(job1, new Path("top10pages")); // Output: intermediate top 10 pages

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // **Job 2: Map-Side Join with Pages File Using Cached Top 10 Pages**
        Job job2 = Job.getInstance(conf, "Debug: Map-Side Join with Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(PageJoinMapper.class);
        job2.setReducerClass(UniqueReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        // Now use pages.csv (which is large) as input.
        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input: pages.csv
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));  // Final output

        // Add the small top10pages file (from Job 1 output) to the distributed cache.
        job2.addCacheFile(new Path("top10pages/part-r-00000").toUri());

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    // Main method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Delete any pre-existing output directories
        deleteOutputIfExists(conf, "top10pages");
        deleteOutputIfExists(conf, args[2]);

        // --- Job 1: Access Log Aggregation ---
        Job job1 = Job.getInstance(conf, "Access Log Aggregation");
        job1.setJarByClass(TaskB.class);
        job1.setMapperClass(AccessLogMapper.class);
        job1.setReducerClass(AccessLogReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[0])); // Input: access_logs.csv
        FileOutputFormat.setOutputPath(job1, new Path("top10pages")); // Output: intermediate top 10 pages

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // --- Job 2: Map-Side Join with Pages using Cached Top 10 Pages ---
        Job job2 = Job.getInstance(conf, "Map-Side Join with Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(PageJoinMapper.class);
        job2.setReducerClass(UniqueReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        // Now use pages.csv as input (pages is large, so we process it normally)
        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input: pages.csv
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));  // Final output directory

        // Add the small top 10 pages file to the distributed cache.
        job2.addCacheFile(new Path("top10pages/part-r-00000").toUri());

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}