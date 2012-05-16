package org.swk;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeansClassifer {

	public static class KMeansMapper extends 
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		public enum UpdateCounter {
			UPDATE
		}
		
		public static final IntWritable one  = new IntWritable(1);
		public              Text        word = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			
			FileSplit fs = (FileSplit)context.getInputSplit();
			String filename = fs.getPath().toString();
			System.out.println(filename);
			String tmp = "file:/Users/tomo/Documents/workspace/KMeansClassifer/kmeans/depth_1/part-r-00000";
			
			if (!filename.equals(tmp))
				context.getCounter(UpdateCounter.UPDATE).increment(1);
			
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class KMeansReducer extends 
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable value : values	 ) 
				sum += value.get();
			
			context.write(key, new IntWritable(sum));
		}
		
	}
	
			
	public static void main(String[] args) throws Exception {
		int  depth   = 1;
		long counter = 1;
		
		Configuration conf;
		
		String inputdir;
		String outputdir;
		Path inputpath;
		Path outputpath;
		
		while (counter > 0) {
			inputdir   = "kmeans/depth_" + (depth - 1) + "/";
			outputdir  = "kmeans/depth_" + depth;
			inputpath  = new Path(inputdir);
			outputpath = new Path(outputdir);
			
			conf   = new Configuration();
			conf.set("recursion.depth", depth + "");

			Job job = new Job(conf, "KMeans[" + depth + "]: inputdir=" + inputdir + " outputdir=" + outputdir);

			job.setJarByClass(KMeansClassifer.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setNumReduceTasks(1);

			delete(new File(outputdir));
			
			FileInputFormat.setInputPaths(job, inputpath);
			FileOutputFormat.setOutputPath(job, outputpath);

			job.waitForCompletion(true);
			
			depth++;
			counter = job.getCounters().findCounter(KMeansMapper.UpdateCounter.UPDATE).getValue();
		}
	}
	
	static private void delete(File f) {
		if (!f.exists()) return;
		
		if (f.isFile()) f.delete();
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			
			for(int i = 0; i < files.length; i++) 
				delete(files[i]);
			
			f.delete();
		}
	}

}

