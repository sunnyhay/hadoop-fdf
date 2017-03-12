package com.gdf.fdf;

// parsing is not suitable for map-reduce due to data dependency in FDF raw data. so use sequential parser and map-reduce parsed results.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LoadGDFParser extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LoadGDFParser(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		Job job = Job.getInstance(conf);
		job.setJobName("load GDF");
		job.setJarByClass(LoadGDFParser.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// job.setNumReduceTasks(2);
		
		job.setMapperClass(GDFParserMap.class);
		job.setReducerClass(GDFParserReduce.class);
		
		// job.setCombinerClass(Reduce.class);
		
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
