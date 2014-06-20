import java.io.*;
import java.util.*;
import java.math.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Data Mining Homework 5 - exercise 3
 * KMeans clutering: assign a random number to each doc
 * @author Federico Lombardi
 */
 
public class MapperRandom extends  Mapper<LongWritable, Text, IntWritable, Text>{
	
	private Text out = new Text();
	private final static IntWritable random = new IntWritable(); 
	
	public void  map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] token = value.toString().split("\t");
		if(token.length != 1){
			int val = (int)(Math.random()*10000);
			random.set(val);
			context.write(random, value);
		}
		
	}
}
