package com.gdf.fdf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GDFTransformReduce extends Reducer<Text, Text, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		
		for (Text value : values) {
			String line = value.toString();
			if (!line.equals("no use"))
				count ++;
		}
		System.out.println("in reducer key: " + key.toString() + " with count: " + count);
		
		context.write(key, new IntWritable(count));
	}
}
