import java.lang.System;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TeraSort{
  public static class TSMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String line = value.toString();
      String Mkey = line.substring(0,10);
      String Mvalue = line.substring(10);
      context.write(new Text(Mkey), new Text(Mvalue));
    }
  }

  public static class TSReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Text value, Context context)
    throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

    public static void main(String[] args) throws Exception{
      if(args.length !=2){
        System.err.println("Usage: TeraSort <input path> <output path>");
        System.exit(-1);
      }
      long start=0;
      long end=0;
      start = System.currentTimeMillis();
      //Define mapreduce job
      Job job = new Job();
      job.setJarByClass(TeraSort.class);
      job.setJobName("TeraSort");

      //set input and output location
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      //set input and output formats
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      //Set Mapper and reducer
      job.setMapperClass(TSMapper.class);
      job.setReducerClass(TSReducer.class);

      //Output types
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      //Submit job
      System.exit(job.waitForCompletion(true)? 0 : 1);
      end = System.currentTimeMillis();
      System.out.println("Total Time taken for Execution :" + (end-start)/1000);
    }
}
