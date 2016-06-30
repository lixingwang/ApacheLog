package com.tracy.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount2 {

    public static final String DATA = "192.168.11.4 - 100001887 [26/Apr/2000:00:23:50 -0400] \"GET /bank/creditsecured.gif HTTP/1.0\" 200 1031 \"http://www.ban.com/creditsecured\" \"Mozilla/4.05 (Macintosh; I; PPC)\"";

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String logEntryPattern ="^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
            Pattern logPattern = Pattern.compile(logEntryPattern);
            Matcher matcher = logPattern.matcher(value.toString());
            if(matcher.matches()) {
                String userId = matcher.group(3);
                String creditTypeLink = matcher.group(5);
                String creditType = creditTypeLink.substring(creditTypeLink.lastIndexOf("/bank/") + 6, creditTypeLink.lastIndexOf(".gif"));
                System.out.println("User ID: " + userId);
                System.out.println("Credit Type: " + creditType);
                word.set(userId + " " + creditType);
                context.write(word, one);
            }else {
                System.out.println("Ignore line since it's format error: " + value);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
//
////        String patter = """^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-]\d{4})\] "(.+?)" (\d{3}) ([\d\-]+) "([^"]+)" "([^"]+)".*""";
////        String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*";
////        String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
//        String logEntryPattern ="^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
//
//        Pattern logPattern = Pattern.compile(logEntryPattern);
//        Matcher matcher = logPattern.matcher(DATA);
//        if(matcher.matches()) {
//            String userId = matcher.group(1);
//            System.out.println("User ID: " + userId);
//            System.out.println("Credit Type: " +  matcher.group(5));
//        }

//        args = new String[]{"D:/wordcount.txt", "D:/result"};
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}