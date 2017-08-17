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
 * ͳ������ÿ������վ30���ƽ������ 
 * 1.��дmap�������� 
 * 2.��дreduce�������� 
 * 3.��дrun�������� 
 * 4.��main�������������г���
 * 
 * @author Administrator
 *
 */
public class Temperature extends Configured implements Tool {
	public static class TemperatureMapper extends Mapper<LongWritable, Text/* String */, Text, IntWritable/* int */> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// ��ÿ������վ����ת��ΪString
			String line = value.toString();
			// �ڶ�������ȡ����
			int tempertaure = Integer.parseInt(line.substring(14, 19).trim());
			if (tempertaure != -9999) {
				// ��ȡ����վ���
				// ��ȡ�����Ƭ
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				// ��ȡ����վ���
				String weatherStationId = fileSplit.getPath().getName().substring(5, 10);

				// �������
				context.write(new Text(weatherStationId), new IntWritable(tempertaure));
			}
		}
	}

	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// ͳ����ͬ����վ����������ֵ
			int sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				// ����������ֵ�ۼ�
				sum += val.get();
				// ͳ�Ƽ��ϴ�С
				count++;
			}
			// ��ͬһ������վ��ƽ��ֵ
			result.set(sum / count);

			// �������
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// ��ȡ�����ļ�
		Configuration conf = new Configuration();
		// ���·�����ھ�ɾ��
		Path mypath = new Path(arg0[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		// ����job����
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "temperature");
		job.setJarByClass(Temperature.class);

		// ָ����������·�������·��
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		// ָ��Mapper��Reducter
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);

		// ����map������Reduce�����������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// �ύ��ҵ
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// ���������������
		String[] args0 = { "hdfs://zhm:9000/weather/", "hdfs://zhm:9000/weather/out" };
		// ����runִ�г���
		int ec = ToolRunner.run(new Configuration(), new Temperature(), args0);
		// ���ݷ���״̬�˳�
		System.exit(ec);
	}
}
