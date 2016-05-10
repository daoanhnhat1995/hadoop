/**
 * 
 * @author Nhat Dao
 *
 * 09/05/2016
 * CS 4331
 * Dataset: Gov Census Weight Trend data
 * This Map Reduce program takes in a large file of data (8Gb)
 * Output average_weight by {year,state,gender}
 *
 * >> Example:
 *    Year: 2009 | State: 1 | Gender: 2 | Average Weight: 20
 * 
 *
 */


import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class WeightTrend extends Configured implements Tool{

 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

				String str = value.toString();
				String[] strList = str.split(",");

				//Ignore line with labels
				if(Character.isDigit(str.charAt(0))){

					// SERIALNO contains first 4 chars as num year
					String year = strList[0].toString().substring(0, 4);
					String state = strList[5].toString();
					int pwgtp = Integer.parseInt(strList[7]);
					double sex = Double.parseDouble(strList[69].toString());

					String keyValue = "Year: " + year + "\t| State : " + state + "\t| Gender: " + sex + "\t| Average weight: ";

					context.write(new Text(keyValue),new IntWritable(pwgtp));
				}

    }

  public void run (Context context) throws IOException, InterruptedException {
        setup(context);
        while (context.nextKeyValue()) {
              map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
        cleanup(context);
  }
 }

 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        int sum = 0;
				int count = 0;
        for (IntWritable val : values) {
            sum += val.get();
						count ++;
        }
				int average = sum/count;
        context.write(key, new IntWritable(average));
    }
 }

public int run(String[] args) throws Exception {

    Job job = Job.getInstance(new Configuration());

    job.setJobName("weighttrend");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(WeightTrend.class);

    job.submit();
    return 0;
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    ToolRunner.run(new WeightTrend(), otherArgs);
 }
}
