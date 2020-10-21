package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

	  // Sum the values and divide by count
	  double d = 0;
	  int count = 0;
	  for (DoubleWritable val: values){
		  d += val.get();
		  count++;
	  }
	  context.write(key, new DoubleWritable(d / count));
  }  
}
