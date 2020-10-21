package example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NameYearPartitioner<K2, V2> extends
		HashPartitioner<StringPairWritable, Text> {

	/**
	 * Partition Name/Year pairs according to the first string (last name) in the string pair so 
	 * that all keys with the same last name go to the same reducer, even if  second part
	 * of the key (birth year) is different.
	 */
	public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {
		// Partitioning based on first character of the last name
		// In the case of 2 reduce tasks, this will put first 13
		// letters in reducer 0 and others in reducer 1
		// Also accounts for varying number of reduce tasks
		
		return (key.getLeft().toLowerCase().charAt(0) - 'a') * numReduceTasks / 26;
		
		// // Previous implementation
		// return (key.getLeft().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}
