package com.autohome.count;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;





public class CountMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter("TONGJI", "map").increment(1);
	}
}

