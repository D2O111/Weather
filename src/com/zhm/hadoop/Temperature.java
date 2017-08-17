package com.zhm.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 统计美国每个气象站30年的平均气温 
 * 1.编写map（）函数 
 * 2.编写reduce（）函数 
 * 3.编写run（）函数 
 * 4.在main（）方法中运行程序
 * 
 * @author Administrator
 *
 */
public class Temperature extends Configured implements Tool {
	public static class TemperatureMapper extends Mapper<LongWritable, Text/* String */, Text, IntWritable/* int */> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 将每行气象站数据转换为String
			String line = value.toString();
			// 第二部，提取气温
			int tempertaure = Integer.parseInt(line.substring(14, 19).trim());
			if (tempertaure != -9999) {
				// 获取气象站编号
				// 获取输入分片
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				// 获取气象站编号
				String weatherStationId = fileSplit.getPath().getName().substring(5, 10);

				// 输出数据
				context.write(new Text(weatherStationId), new IntWritable(tempertaure));
			}
		}
	}

	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// 统计相同气象站的所有气温值
			int sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				// 对所有气温值累加
				sum += val.get();
				// 统计集合大小
				count++;
			}
			// 求同一个气象站的平均值
			result.set(sum / count);

			// 输出数据
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 读取配置文件
		Configuration conf = new Configuration();
		// 输出路径存在就删除
		Path mypath = new Path(arg0[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		// 构建job对象
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "temperature");
		job.setJarByClass(Temperature.class);

		// 指定数据输入路径和输出路径
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		// 指定Mapper和Reducter
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);

		// 设置map函数和Reduce函数输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 提交作业
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// 定义输入输出数据
		String[] args0 = { "hdfs://zhm:9000/weather/", "hdfs://zhm:9000/weather/out" };
		// 调用run执行程序
		int ec = ToolRunner.run(new Configuration(), new Temperature(), args0);
		// 根据返回状态退出
		System.exit(ec);
	}
}
