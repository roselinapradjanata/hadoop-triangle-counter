import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TriangleCounter extends Configured implements Tool {

	public static class PreprocessingMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		Text mKey = new Text();
		LongWritable zero = new LongWritable(0);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			long u = Long.parseLong(tokenizer.nextToken());
			long v = Long.parseLong(tokenizer.nextToken());

			mKey.set(u + "," + v);
			context.write(mKey, zero);
			
			mKey.set(v + "," + u);
			context.write(mKey, zero);
		}
	}

	public static class PreprocessingReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable> {
		LongWritable mKey = new LongWritable();
		LongWritable mValue = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			String line = key.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, ",");

			long u = Long.parseLong(tokenizer.nextToken());
			long v = Long.parseLong(tokenizer.nextToken());
			
			mKey.set(u);
			mValue.set(v);

			context.write(mKey, mValue);
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

	public static class TriadsReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
		LongWritable zero = new LongWritable(0);
		Text rValue = new Text();
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

				rValue.set(key.get() + "," + e);
				context.write(zero, rValue);
			}

			Arrays.sort(arr, 0, size);

			for (int i = 0; i < size; i++) {
				for (int j = i + 1; j < size; j++) {
					rValue.set(arr[i] + "," + arr[j]);
					context.write(key, rValue);
				}
			}
		}
	}

	public static class TriadsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		Text mKey = new Text();
		LongWritable mValue = new LongWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			mValue.set(Long.parseLong(tokenizer.nextToken()));
			mKey.set(tokenizer.nextToken());

			context.write(mKey, mValue);
		}
	}

	public static class CountTrianglesReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable> {
		LongWritable rKey = new LongWritable();
		LongWritable one = new LongWritable(1);

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<LongWritable> vs = values.iterator();
			ArrayList<Long> arr = new ArrayList<Long>();
			
			while (vs.hasNext()) {
				arr.add(vs.next().get());
			}

			if (arr.contains(0)) {
				for (long value : arr) {
					if (value != 0) {
						rKey.set(value);
						context.write(rKey, one);
					}
				}
			}
		}
	}

	public static class AggregateCountReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		LongWritable mKey = new LongWritable();
		LongWritable mValue = new LongWritable();

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<LongWritable> vs = values.iterator();

			long count = 0;
			while (vs.hasNext()) {
				count += vs.next().get();
			}

			mKey.set(key.get());
			mValue.set(count);

			context.write(mKey, mValue);
		}
	}

	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), "preprocessing");
		job1.setJarByClass(TriangleCounter.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(LongWritable.class);

		job1.setMapperClass(PreprocessingMapper.class);
		job1.setReducerClass(PreprocessingReducer.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("/user/roselina/temp1"));

		Job job2 = Job.getInstance(getConf(), "triads");
		job2.setJarByClass(TriangleCounter.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(LongWritable.class);

		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(PairsMapper.class);
		job2.setReducerClass(TriadsReducer.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path("/user/roselina/temp1"));
		FileOutputFormat.setOutputPath(job2, new Path("/user/roselina/temp2"));

		Job job3 = Job.getInstance(getConf(), "triangles");
		job3.setJarByClass(TriangleCounter.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(LongWritable.class);

		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);

		job3.setMapperClass(TriadsMapper.class);
		job3.setReducerClass(CountTrianglesReducer.class);

		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path("/user/roselina/temp2"));
		FileOutputFormat.setOutputPath(job2, new Path("/user/roselina/temp3"));

		Job job4 = Job.getInstance(getConf(), "count");
		job4.setJarByClass(TriangleCounter.class);

		job4.setOutputKeyClass(LongWritable.class);
		job4.setOutputValueClass(LongWritable.class);

		job4.setReducerClass(AggregateCountReducer.class);

		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job4, new Path("/user/roselina/temp3"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]));

		int ret = job1.waitForCompletion(true) ? 0 : 1;
		if (ret == 0) ret = job2.waitForCompletion(true) ? 0 : 1;
		if (ret == 0) ret = job3.waitForCompletion(true) ? 0 : 1;
		if (ret == 0) ret = job4.waitForCompletion(true) ? 0 : 1;
		return ret;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TriangleCounter(), args);
		System.exit(res);
	}
}
