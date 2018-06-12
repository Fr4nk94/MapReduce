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
public class MaxWord {

	/* Mapper */
	static class MapperCount extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split(" ");

			for (String word : words) {
				if(!word.equals("  "))
				{
				Text outputKey = new Text(word.toUpperCase().trim());
				LongWritable outputValue = new LongWritable(1);
				context.write(outputKey, outputValue);
				}
			}
		}

	}

	/* Reducer */
	static class ReducerCount extends Reducer<Text, LongWritable, Text, LongWritable> {

		Text wordMax = new Text();
		int occurenceOfWordMax = 0;

		
		@Override
		public void reduce(Text word, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			
			if (count > occurenceOfWordMax) {
				occurenceOfWordMax = count;
				wordMax.set(word.toString());
			
			}
		}

	
		@Override
		protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(new Text("MAX: " + wordMax.toString() + " FREQUENCY: "), new LongWritable(occurenceOfWordMax));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);

		Job job = Job.getInstance(conf, "wordcount");
		job.setJarByClass(MaxWord.class);
		job.setMapperClass(MapperCount.class);
		job.setReducerClass(ReducerCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
