package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends
		Mapper<LongWritable, Text, StringPairWritable, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// First split by word and then emit a key, value with 
		// first two words as composite key
		String[] s = value.toString().split("\\W+");
		context.write(new StringPairWritable(s[0], s[1]), new LongWritable(1));

	}
}
