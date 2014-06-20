import  java.io.*;
import java.util.*; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Data Mining Homework 5 - exercise 3
 * KMeans clustering: cluster representation
 * @author Federico Lombardi
 */
 
public class ReducerClusterVisualization extends Reducer<Text,Text, Text, Text>{
	
	private Text word = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		Iterator it = values.iterator();
		String cluster = "";
		while(it.hasNext()){
			String value = it.next().toString();
			String[] token = value.split("\t");
			String idTerm = token[0];
			cluster += idTerm + " ";
		}
		word.set(cluster);
		context.write(key, word);
	}

}