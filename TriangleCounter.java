import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TriangleCounter extends Configured implements Tool {

	public static class PreprocessingMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		LongWritable mKey = new LongWritable();
		LongWritable mValue = new LongWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			long u = Long.parseLong(tokenizer.nextToken());
			long v = Long.parseLong(tokenizer.nextToken());

			mKey.set(u);
			mValue.set(v);
			
			context.write(mKey, mValue);
			context.write(mValue, mKey);
		}
	}

	public static class PairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		LongWritable mKey = new LongWritable();
		LongWritable mValue = new LongWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			long u = Long.parseLong(tokenizer.nextToken());
			long v = Long.parseLong(tokenizer.nextToken());

			if (u < v) {
				mKey.set(u);
				mValue.set(v);
				context.write(mKey, mValue);
			}
		}
	}

	public static class TriadsReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
		Text rKey = new Text();
		LongWritable zero = new LongWritable(0);
		long[] arr = new long[4096];
		int size = 0;

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<LongWritable> vs = values.iterator();
			for (size = 0; vs.hasNext(); size++) {
				if (arr.length == size) {
					arr = Arrays.copyOf(arr, arr.length * 2);
				}

				long e = vs.next().get();
				arr[size] = e;

				rKey.set(key.get() + "," + e);
				context.write(rKey, zero);
			}

			Arrays.sort(arr, 0, size);

			for (int i = 0; i < size; i++) {
				for (int j = i + 1; j < size; j++) {
					rKey.set(arr[i] + "," + arr[j]);
					context.write(rKey, key);
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), "preprocessing");
		job1.setJarByClass(TriangleCounter.class);

		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setMapperClass(PreprocessingMapper.class);
		job1.setNumReduceTasks(0);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("nyancat"));

		Job job2 = Job.getInstance(getConf(), "triads");
		job2.setJarByClass(TriangleCounter.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(LongWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);

		job2.setMapperClass(PairsMapper.class);
		job2.setReducerClass(TriadsReducer.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path("nyancat"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		int ret = job1.waitForCompletion(true) ? 0 : 1;
		if (ret == 0) ret = job2.waitForCompletion(true) ? 0 : 1;
		return ret;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TriangleCounter(), args);
		System.exit(res);
	}
}
