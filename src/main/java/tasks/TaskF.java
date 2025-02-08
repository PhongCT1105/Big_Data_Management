package tasks;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TaskF {

    public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private boolean isHeader = true;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (isHeader && line.startsWith("FriendRel")) {
                isHeader = false;
                return;
            }
            isHeader = false;

            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                String p1 = tokens[1].trim();
                String p2 = tokens[2].trim();
                outKey.set(p1);
                outValue.set("F:" + p2);
                context.write(outKey, outValue);
            }
        }
    }

    public static class VisitMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private boolean isHeader = true;  // To skip the header line

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (isHeader && line.startsWith("AccessID")) {
                isHeader = false;
                return;
            }
            isHeader = false;

            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                String p1 = tokens[1].trim();
                String p2 = tokens[2].trim();
                outKey.set(p1);
                outValue.set("V:" + p2);
                context.write(outKey, outValue);
            }
        }
    }

    public static class NeglectfulFriendsReducer extends Reducer<Text, Text, Text, Text> {
        // Map from PersonID to Name loaded from the Distributed Cache.
        private Map<String, String> personDetails = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    Path path = new Path(cacheFile.getPath());
                    BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
                    String line;
                    boolean isHeader = true;
                    while ((line = reader.readLine()) != null) {
                        if (isHeader && line.startsWith("PersonID")) {
                            isHeader = false;
                            continue;
                        }
                        isHeader = false;
                        String[] tokens = line.split(",");
                        if (tokens.length >= 2) {
                            String personId = tokens[0].trim();
                            String name = tokens[1].trim();
                            personDetails.put(personId, name);
                        }
                    }
                    reader.close();
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> friends = new HashSet<>();
            Set<String> visits = new HashSet<>();

            for (Text val : values) {
                String record = val.toString();
                if (record.startsWith("F:")) {
                    friends.add(record.substring(2));
                } else if (record.startsWith("V:")) {
                    visits.add(record.substring(2));
                }
            }

            if (!friends.isEmpty()) {
                boolean visitedAnyFriend = false;
                for (String friend : friends) {
                    if (visits.contains(friend)) {
                        visitedAnyFriend = true;
                        break;
                    }
                }
                if (!visitedAnyFriend) {
                    String p1 = key.toString();
                    String name = personDetails.get(p1);
                    if (name == null) {
                        name = "Unknown";
                    }
                    context.write(new Text(p1), new Text(name));
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: TaskF <friendInput> <visitInput> <personDetails> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Neglectful Friends");
        job.setJarByClass(TaskF.class);

        job.addCacheFile(new Path(otherArgs[2]).toUri());

        job.setReducerClass(NeglectfulFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, FriendMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, VisitMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public void debug(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TaskF debug <friendInput> <visitInput> <output> <personDetails>");
            return;
        }
        String friendInput   = args[0];
        String visitInput    = args[1];
        String output        = args[2];
        String personDetails = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Neglectful Friends Debug");
        job.setJarByClass(TaskF.class);

        job.addCacheFile(new Path(personDetails).toUri());

        job.setReducerClass(NeglectfulFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(friendInput), TextInputFormat.class, FriendMapper.class);
        MultipleInputs.addInputPath(job, new Path(visitInput), TextInputFormat.class, VisitMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean success = job.waitForCompletion(true);
        System.out.println("Debug Job " + (success ? "succeeded" : "failed"));
    }
}
