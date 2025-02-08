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

    private static void deleteOutputIfExists(Configuration conf, String pathStr) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(pathStr);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }
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

    public static class AccessLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public static class PageCount {
            String pageID;
            int count;
            public PageCount(String id, int cnt) {
                this.pageID = id;
                this.count = cnt;
            }
        }
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
            List<PageCount> topList = new ArrayList<>(topPagesHeap);
            topList.sort((a, b) -> Integer.compare(b.count, a.count));
            for (PageCount pc : topList) {
                context.write(new Text(pc.pageID), new IntWritable(pc.count));
            }
        }
    }


    public static class PageJoinMapper extends Mapper<Object, Text, Text, Text> {
        private HashSet<String> topPages = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path topPagesCacheFile = cacheFiles[0];
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(topPagesCacheFile)));
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
                String pageID = fields[0].trim();
                if (topPages.contains(pageID)) {
                    String pageData = fields[1].trim() + "," + fields[2].trim();
                    context.write(new Text(pageID), new Text(pageData));
                }
            }
        }
    }

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

        deleteOutputIfExists(conf, "top10pages");
        deleteOutputIfExists(conf, args[2]);

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

        Job job2 = Job.getInstance(conf, "Debug: Map-Side Join with Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(PageJoinMapper.class);
        job2.setReducerClass(UniqueReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input: pages.csv
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));  // Final output

        job2.addCacheFile(new Path("top10pages/part-r-00000").toUri());

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        deleteOutputIfExists(conf, "top10pages");
        deleteOutputIfExists(conf, args[2]);

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

        Job job2 = Job.getInstance(conf, "Map-Side Join with Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(PageJoinMapper.class);
        job2.setReducerClass(UniqueReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input: pages.csv
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));  // Final output directory

        job2.addCacheFile(new Path("top10pages/part-r-00000").toUri());

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}