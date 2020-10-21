package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	boolean caseSensitive = true;
		
	public void setup(Context context){
		// get configuration from context and get caseSensitive param
		// with true as default
		Configuration conf = context.getConfiguration();
		this.caseSensitive = conf.getBoolean("caseSensitive", true);
		
		
	}

	@Override
    public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException { 

		// grab the string representation
        String line = value.toString();
	  
		for (String word: line.split("\\W+")){
			if (word.length() > 0){
				
				// get the first letter as string
				String str = word.substring(0, 1);
				
				// if caseSensitive is false change to lower case
				if (!this.caseSensitive){
					str = str.toLowerCase();
				}
				
				context.write(new Text(str), new DoubleWritable(word.length()));
			}
		}

	}
}
