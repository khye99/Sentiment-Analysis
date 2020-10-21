package stubs;

import java.io.File;
import java.net.URI;
import org.apache.hadoop.fs.Path;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;


public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
	  this.configuration = configuration;
	  try{
		  // Open a scanner for positive file
		  Scanner posFile = new Scanner(new File("positive-words.txt"));
		  
		  // scan the file
		  while(posFile.hasNext()){
			  String s = posFile.next();
			  
			  // if it does not start with ';', we add it to the respective set
			  if (s.charAt(0) != ';'){
				  positive.add(s);
				  System.out.println(s);
			  }
		  }
		  posFile.close();
		  
		  // do same thing for negative words file
		  Scanner negFile = new Scanner(new File("negative-words.txt"));
		  while(negFile.hasNext()){
			  String s = negFile.next();
			  if (s.charAt(0) != ';'){
				  negative.add(s);
			  }
		  }
		  negFile.close();
	  } catch(Exception e){
		  e.printStackTrace();
	  }
	  
	  
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	  
	  // get the key
	  String word = key.toString();
	  // if a set contains the word, we return corresponding word, else we return 2
	  if (positive.contains(word)) return 0;
	  if(negative.contains(word)) return 1;
      return 2;
  }
}
