package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives words as key and a set
 * of locations in the form "play name@line number" as the values.
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word.
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {

		// create a string representation of all instances
		// of the work and emit the key, value pair
		StringBuilder sb = new StringBuilder();
		for (Text value: values){
			sb.append(value.toString()).append(",");
		}
		// delete the last stray comma
		sb.deleteCharAt(sb.length() - 1);
		context.write(key, new Text(sb.toString()));
	}
}
