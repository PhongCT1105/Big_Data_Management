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

    // Mapper Class: Extracts WhatPage as Key and 1 as Value (PageID, 1)
    public static class AccessLogMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text pageID = new Text();
        private final static IntWritable one = new IntWritable(1);
        private boolean isFirstLine = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            if (fields.length >= 3) {
                pageID.set(fields[2].trim());
                context.write(pageID, one);
            }
        }
    }

    // Reducer: Aggregates total accesses and extracts top 10 pages
    public static class AccessLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Store top pages: TreeMap sorted by count (descending), and PageID (ascending)
        private TreeMap<Integer, TreeSet<String>> topPages = new TreeMap<>(Comparator.reverseOrder());

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Insert into TreeMap: Group pages with same count and order by PageID
            topPages.putIfAbsent(sum, new TreeSet<>()); // TreeSet maintains ascending order of PageID
            topPages.get(sum).add(key.toString());

            // Keep only top 10 entries in total
            if (topPages.size() > 10) {
                int totalEntries = 0;
                TreeMap<Integer, TreeSet<String>> newTopPages = new TreeMap<>(Comparator.reverseOrder());

                for (Map.Entry<Integer, TreeSet<String>> entry : topPages.entrySet()) {
                    for (String pageID : entry.getValue()) {
                        if (totalEntries < 10) {
                            newTopPages.putIfAbsent(entry.getKey(), new TreeSet<>());
                            newTopPages.get(entry.getKey()).add(pageID);
                            totalEntries++;
                        } else {
                            break;
                        }
                    }
                }
                topPages = newTopPages; // Replace with filtered top 10 results
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int count = 0;

            for (Map.Entry<Integer, TreeSet<String>> entry : topPages.entrySet()) {
                for (String pageID : entry.getValue()) {
                    if (count++ < 10) { // Output only the first 10
                        context.write(new Text(pageID), new IntWritable(entry.getKey()));
                    } else {
                        return; // Stop after 10 entries
                    }
                }
            }
        }
    }


    // Mapper for Map-Side Join with pages.csv
    public static class PageJoinMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> pageInfo = new HashMap<>();
        private HashSet<String> topPages = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());

            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length >= 3) {
                        pageInfo.put(fields[0], fields[1] + "," + fields[2]);
                    }
                }
                reader.close();
            }

            Path topPagesPath = new Path("top10pages/part-r-00000");
            if (fs.exists(topPagesPath)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(topPagesPath)));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\t");
                    if (fields.length == 2) {
                        topPages.add(fields[0]);
                    }
                }
                reader.close();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length >= 3) {
                String pageID = fields[2].trim();
                if (topPages.contains(pageID)) {
                    String pageData = pageInfo.getOrDefault(pageID, "Unknown,Unknown");
                    context.write(new Text(pageID), new Text(pageData));
                }
            }
        }
    }

    // Reducer to Remove Duplicates
    public static class UniqueReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
                break;
            }
        }
    }


    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // **Delete existing output directories before running the job**
        deleteOutputIfExists(conf, "top10pages");
        deleteOutputIfExists(conf, args[2]);

        // **First Job: Extract Top 10 Pages**
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
        FileOutputFormat.setOutputPath(job1, new Path("top10pages")); // Output: intermediate top 10

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // **Second Job: Map-Side Join + Deduplication**
        Job job2 = Job.getInstance(conf, "Debug: Map-Side Join with Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(PageJoinMapper.class);
        job2.setReducerClass(UniqueReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[0])); // Reuse access log input
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // Final output

        job2.addCacheFile(new Path(args[1]).toUri()); // Add pages.csv to distributed cache

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // **Delete output directories before running**
        deleteOutputIfExists(conf, "top10pages");
        deleteOutputIfExists(conf, args[2]);

        // **First Job: Extract Top 10 Pages**
        Job job1 = Job.getInstance(conf, "Access Log Aggregation");
        job1.setJarByClass(TaskB.class);
        job1.setMapperClass(AccessLogMapper.class);
        job1.setReducerClass(AccessLogReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("top10pages"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // **Second Job: Map-Side Join + Deduplication**
        Job job2 = Job.getInstance(conf, "Map-Side Join with Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(PageJoinMapper.class);
        job2.setReducerClass(UniqueReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.addCacheFile(new Path(args[1]).toUri());

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
