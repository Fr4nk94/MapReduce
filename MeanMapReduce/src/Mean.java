import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/* Word Count with new Interface */
public class Mean {

	//for each word assign the value 1 --> MAPPER
	//START FIRST JOB
	static class MapperCount  extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split("");

			for (String word : words) {
				Text outputKey = new Text(word.toUpperCase().trim());
				LongWritable outputValue = new LongWritable(1);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	//for each key we get sum of that key 
	static class ReducerCount extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text word, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}

			context.write(word, new LongWritable(count));
		}
	}

	///END FIRST JOB


	//START SECOND JOB
	static class MapperMean extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split("\t");

			LongWritable wordToLong = new LongWritable(Integer.parseInt(words[0]));
			LongWritable wordValue = new LongWritable(Integer.parseInt(words[1]));

			context.write(new Text((wordToLong.get() * wordValue.get()) + ""), wordValue);

		}
	}


	static class ReducerMean extends Reducer<Text, LongWritable, Text, LongWritable> {

		long sumOfKeys = 0, sumOfValues = 0;

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context arg2)
				throws IOException, InterruptedException {

			for (LongWritable argValue : values) {
				sumOfKeys += Integer.parseInt(key.toString());
				sumOfValues += argValue.get();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			context.write(new Text("MEAN: "), new LongWritable(sumOfKeys / sumOfValues));
		}
	}

	//END SECOND JOB


	static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

		long sumKey = 0, sumValue = 0;

		@Override
		protected void reduce(Text value, Iterable<LongWritable> arg1, Context arg2)
				throws IOException, InterruptedException {

			for (LongWritable argValue : arg1) {
				sumKey += Integer.parseInt(value.toString());
				sumValue += argValue.get();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			context.write(new Text(sumKey + ""), new LongWritable(sumValue));
		}
	}





	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path inputTemp = new Path(files[1]);
		Path outputFinal = new Path(files[2]);

		Job jobCount = Job.getInstance(conf, "JOB_COUNT");
		jobCount.setJarByClass(Mean.class);
		jobCount.setMapperClass(MapperCount.class);
		jobCount.setReducerClass(ReducerCount.class);
		jobCount.setOutputKeyClass(Text.class);
		jobCount.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(jobCount, input);
		FileOutputFormat.setOutputPath(jobCount, inputTemp);

		boolean success = jobCount.waitForCompletion(true);
		if (success) {
			Job jobMean = Job.getInstance(conf, "JOB_MEAN");
			jobMean.setMapperClass(MapperMean.class);
			jobMean.setCombinerClass(Combiner.class);
			jobMean.setReducerClass(ReducerMean.class);

			jobMean.setOutputKeyClass(Text.class);
			jobMean.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(jobMean, inputTemp);
			FileOutputFormat.setOutputPath(jobMean, outputFinal);
			success = jobMean.waitForCompletion(true);
		}

		if (success)
			System.exit(1);
		else
			System.exit(0);
	}

}
