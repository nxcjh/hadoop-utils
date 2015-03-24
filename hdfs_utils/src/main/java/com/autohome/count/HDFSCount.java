package com.autohome.count;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;





public class HDFSCount {

	private static Configuration conf ;
	private static FileSystem fs ;

	public static void main(String[] args) throws IOException {
	
		conf = new Configuration();
		fs = FileSystem.get(conf);
		Job job = new Job(conf);
		job.setJarByClass(HDFSCount.class);
		job.setJobName("count");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(CountMapper.class);
//		job.setReducerClass(CheckDataReduce.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//设置输入的文件
		for (String p : listFile(args[0])) {			
			if(p.endsWith("_SUCCESS")){
			}else{
				System.out.println(p);
				FileInputFormat.addInputPath(job, new Path(p));
			}
			
		}
		try {
			boolean flage = job.waitForCompletion(true);
			System.out.println("count:\t"+job.getCounters().findCounter("TONGJI", "map").getValue());
//			System.out.println("map:"+job.getCounters().findCounter("TONGJI", "reduce").getValue());
			fs.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {		
			e.printStackTrace();
		}
	}
	
	/**
	 * 迭代出所有输入目录的中的文件地址
	 * @return 文件地址数组，如果没有文件则返回Null
	 * @throws IOException
	 */
	private static ArrayList<String> listFile( String path) {

		ArrayList<String> filelist = new ArrayList<String>();
		Path p = new Path(path);
		try {
			if (fs.getFileStatus(p).isDir()) {
				FileStatus[] _list = fs.listStatus(p);
				Path[] paths = FileUtil.stat2Paths(_list);
				for (Path _p : paths) {
					for (String s : listFile(_p.toString())) {
						filelist.add(s);
					}
				}
			} else {
				filelist.add(p.toString());
			}
		} catch (FileNotFoundException e) {
			System.err.println("The Input Path Is Not Exists! :"+path);
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return filelist;
	}

}
