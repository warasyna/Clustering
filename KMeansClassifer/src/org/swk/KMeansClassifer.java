package org.swk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeansClassifer {
//	public enum UpdateCounter {
//		UPDATE
//	}

	public static class KMeansMapper extends 
			Mapper<LongWritable, Text, IntWritable, Text> {
		
		public static final IntWritable one  = new IntWritable(1);
		public              Text        word;
		Map<Integer, double[]> centers = new TreeMap<Integer, double[]>();
		
		@Override
		protected void setup(Context context) 
				throws IOException, InterruptedException {
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			Path centroids = new Path(conf.get("centroid.path"));
//			FileSystem fs = FileSystem.get(URI.create(centroids.toString()), conf);
			FileSystem fs = FileSystem.get(conf);
			
			//TODO: Search file api and Refactor based on the information
			InputStream in = fs.open(centroids);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
//			StringBuffer buf = new StringBuffer();
			String str;

			int label = 0;
			while ((str = reader.readLine()) != null) {
				System.out.println("Center:" + label + " = " + str);
				StringTokenizer tokenizer = new StringTokenizer(str);
				int dim = tokenizer.countTokens();
				double[] vector = new double[dim];
				
				for (int i = 0; i < dim; i++)
					vector[i] = Double.parseDouble(tokenizer.nextToken());
				
//				vector[dim] = Double.parseDouble(tokenizer.nextToken());
				
				centers.put(label, vector);
				label++;
				
//				centers.add(map);
			}
			
			in.close();
//			for (Map.Entry<Integer, double[]> e : centers.entrySet()) {
//				System.out.print("label = " + e.getKey() + "vector = [");
//				double[] v = e.getValue();
//				for (int i = 0; i < v.length; i++) {
//					if (i == (v.length - 1)) System.out.println(v[i] + "]");
//					else System.out.print(v[i] + ", ");
//				}
//			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			System.out.println("data:" + str);
			StringTokenizer tokenizer = new StringTokenizer(str);
			
//			FileSplit fs = (FileSplit)context.getInputSplit();
//			String filename = fs.getPath().toString();
//			System.out.println(filename);
//			String tmp = "file:/Users/tomo/Documents/workspace/KMeansClassifer/kmeans/depth_1/part-r-00000";
			
//			if (!filename.equals(tmp))
//				context.getCounter(UpdateCounter.UPDATE).increment(1);
			int dim = tokenizer.countTokens() - 2;
			double[] data = new double[dim];
			int default_label = Integer.parseInt(tokenizer.nextToken());
			String out = "";
			
			for (int i = 0; i < dim; i++) {
				data[i] = Double.parseDouble(tokenizer.nextToken());
				out += data[i] + " ";
			}
			
			out += tokenizer.nextToken();
			
			int label = 99;
			double dis = 0.0, minDis = 100.0;
			int i = 0;
			for (Map.Entry<Integer, double[]> e : centers.entrySet()) {
				dis = distance(data, e.getValue());
				System.out.println("The distance between Center:" + i + " and the data is " + dis);
				if (dis < minDis) {
					minDis = dis;
					label = e.getKey();
				}
				i++;
			}
			
			System.out.println("The label of the data is " + label);
			
			context.write(new IntWritable(label), new Text(out));
//			while (tokenizer.hasMoreTokens()) {
//				word.set(tokenizer.nextToken());
//				context.write(word, one);
//			}
			
		}
		
		//TODO: Confirm how to write local method in mr
		double distance(double[] x, double[] y) {
			double dif, d = 0.0;
			
			for (int i = 0; i < x.length; i++) {
				dif = x[i] - y[i];
				d += dif*dif;
			}
			
			return d;
		}

	}
	
	public static class KMeansReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public enum UpdateCounter {
			UPDATE
		}

		Map<Integer, double[]> centers = new TreeMap<Integer, double[]>();
		Map<Integer, double[]> oldCenters = new TreeMap<Integer, double[]>();

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
//			boolean first = true;
			
			//TODO: Set the number of dimension in advance (in main) 
			int dim = 4;
			int count = 0;

			double[] data;
			// Map 構造に
			double[] center = new double[dim];
			for (int i = 0; i < center.length; i++)
				center[i] = 0.0;
			
			// List<double []> dataList = new LinkedList<double[]>();

			for (Text value : values) {
				System.out.println(value);
				StringTokenizer tokenizer = new StringTokenizer(
						value.toString());

//				if (first) {
//					dim = tokenizer.countTokens() - 1;
//					center = new double[dim + 1];
//
//					for (int i = 0; i < center.length; i++)
//						center[i] = 0.0;
//
//					first = false;
//				}

				data = new double[dim];
				for (int i = 0; i < dim; i++) {
					data[i] = Double.parseDouble(tokenizer.nextToken());
					center[i] += data[i];
				}
				// dataList.add(data);
				context.write(key, value);
				count++;
			}

			System.out.println("The Center:" + key + " = (");
			for (int i = 0; i < center.length; i++) {
				center[i] = center[i] / (double) count;
				System.out.println(center[i] + ", ");
			}
			System.out.println(")");

			centers.put(key.get(), center);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);

			Configuration conf = context.getConfiguration();
			Path centroids = new Path(conf.get("centroid.path"));
			FileSystem fs = FileSystem.get(conf);

			// TODO: Search file api and Refactor based on the information
			InputStream in = fs.open(centroids);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));
			// StringBuffer buf = new StringBuffer();
			String str;

			int label = 0;
			while ((str = reader.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(str);
				int dim = tokenizer.countTokens() - 1;
				double[] vector = new double[dim + 1];

				for (int i = 0; i < dim; i++)
					vector[i] = Double.parseDouble(tokenizer.nextToken());
				
				vector[dim] = Double.parseDouble(tokenizer.nextToken());

				oldCenters.put(label, vector);
				label++;

				// centers.add(map);
			}

			fs.delete(centroids, true);

			OutputStream out = fs.create(centroids);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					out));

			for (Map.Entry<Integer, double[]> e : centers.entrySet()) {
				double[] center = e.getValue();
				String line = "";
				for (int i = 0; i < center.length; i++) {
					if (i == (center.length - 1)) line += center[i] + "\n";
					else line += center[i] + " ";
				}
				writer.write(line);

				if (!Arrays.equals(center, oldCenters.get(e.getKey())))
					context.getCounter(UpdateCounter.UPDATE).increment(1);
			}

			writer.close();
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
		
		Path   center = new Path("kmeans/center/center1.txt");
		
		while (counter > 0) {
			inputdir   = "kmeans/depth_" + (depth - 1) + "/";
			outputdir  = "kmeans/depth_" + depth;
			inputpath  = new Path(inputdir);
			outputpath = new Path(outputdir);
			
			conf   = new Configuration();
			conf.set("centroid.path", center.toString());
			conf.set("recursion.depth", depth + "");

			Job job = new Job(conf, "KMeans[" + depth + "]: inputdir=" + inputdir + " outputdir=" + outputdir);

			job.setJarByClass(KMeansClassifer.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setNumReduceTasks(1);

			delete(new File(outputdir));
			
			FileInputFormat.setInputPaths(job, inputpath);
			FileOutputFormat.setOutputPath(job, outputpath);

			job.waitForCompletion(true);
			
			//TODO: set the counter parameter in findCounter
			depth++;
			counter = job.getCounters().findCounter(KMeansReducer.UpdateCounter.UPDATE).getValue();
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

