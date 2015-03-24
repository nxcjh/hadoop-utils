package com.autohome.redupl;

import java.io.IOException;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReRuplReducer extends Reducer<Text,IntWritable,NullWritable,NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,Reducer<Text, IntWritable, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {

		context.getCounter("TONGJI", "reduce").increment(1);
	}

	
}
