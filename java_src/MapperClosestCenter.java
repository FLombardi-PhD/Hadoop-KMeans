import java.io.*;
import java.util.*;
import java.math.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Data Mining Homework 5 - exercise 3
 * KMeans clustering: map closest center to each doc
 * @author Federico Lombardi
 */
 
public class MapperClosestCenter extends Mapper<LongWritable, Text, Text, Text>{
		
	private Text center = new Text();
	private Text out = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(new Configuration(true));
		String copyDir = KMeans.copyDir;
		FSDataInputStream in = fs.open(new Path(copyDir+"/part-r-00000"));
		BufferedReader reader =  new BufferedReader(new InputStreamReader(in));
		
		HashMap<String, List<Double>> map = new HashMap<String, List<Double>>();
		TreeMap<Double, String> tree = new TreeMap<Double,String>();
		
		while(true){
			List<Double> list = new LinkedList<Double>();
			String s = reader.readLine();
			if(s == null) break;
			String[] tokens = s.split("\t"); 
			String tokenKey = tokens[0];
			String[] iftdf = tokens[1].split(" ");
			for(int i=0; i<iftdf.length; i++){
				double score = Double.parseDouble(iftdf[i]);
				list.add(score);
			}
			map.put(tokenKey, list);
			
		}
		
		Set<String> mapKeys = map.keySet();
		Iterator<String> itMapKeys = mapKeys.iterator();
		String line = value.toString();
		String[] tokens = line.split("\t");
		
		if(tokens.length!=1){
			String token0 = tokens[0];
			HashMap<String,List<Double>> vector = KMeans.buildVector(tokens);
			for(int i=0; i<mapKeys.size(); i++){
				Set<String> vectorKeys = vector.keySet();
				Iterator<String> itVectorKeys = vectorKeys.iterator();
				String term = itMapKeys.next();
				double dist = KMeans.euclideanDistance(vector.get(itVectorKeys.next()), map.get(term));
				tree.put(dist, term);
			}
			
			double closestKey = tree.firstKey();
			String closestElem = tree.get(closestKey);
			List<Double> match = map.get(closestElem);
			
			//vector that represent the center of the cluster
			String newCenter = closestElem; 	
			List<Double> current = vector.get(token0);
			String currVector = token0+"\t"; 
			for(int i=0; i<current.size(); i++){
				currVector += String.valueOf(current.get(i))+" ";
			}
			center.set(newCenter);
			out.set(currVector);
			context.write(center, out);
		}
		
		reader.close();
	}
}
