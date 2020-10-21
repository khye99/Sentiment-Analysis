package stubs;
import java.io.IOException;

import org.apache.directory.api.util.KeyValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {
	private String fileName;
	
	@Override
	public void setup(Context context){
		// set the name of file to file name
		// this way we need to do it only once per mapper.
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException,
		InterruptedException {
		
		// Create Text objects to be used for all key value pairs
		Text newKey = new Text();
		Text newValue = new Text();
			
	    for (String word: value.toString().toLowerCase().split("\\W+")){
				// for each word emit a key value pair
	    	newKey.set(word);
	    	newValue.set(fileName + "@" + key);
	    	context.write(newKey, newValue);
	    }
	} 
}
