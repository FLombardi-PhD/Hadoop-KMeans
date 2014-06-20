import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Data Mining Homework 5 - exercise 3
 * KMeans clustering: get first K docs
 * @author Federico Lombardi
 */
 
public class ReducerRandom extends  Reducer< IntWritable,Text, Text, Text>{
	
	private Text word = new Text();
	private Text num = new Text();

	private int counter = 1;
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String param = conf.get("K");
		
		int K = Integer.parseInt(param);
		
		if(counter <= K){
			String vector = "";
			Iterator itValues = values.iterator();
			String value = itValues.next().toString();
			String[] tokens = value.split("\t");
			String id = tokens[0];
			String[] wordArray = tokens[1].split(",");
			for(int i=0; i<wordArray.length; i++){
				String[] wordScoreArray = wordArray[i].split(":");
				double score = Double.parseDouble(wordScoreArray[1]);
				vector += String.valueOf(score)+" ";
			}
			
			word.set(vector);
			num.set(String.valueOf(counter));
			
			context.write(num, word);
			counter++;
		}
	}

}