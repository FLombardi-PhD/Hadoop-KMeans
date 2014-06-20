import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Data Mining Homework 5 - exercise 3
 * KMeans clustering: main
 * @author Federico Lombardi
 */
 
public  class KMeans {
	
	public static String copyDir = "/kmeans/copy";
	public static String centerDir = "/kmeans/centers";
	
	public static double euclideanDistance(List<Double> list1, List<Double> list2){
        double sum = 0.0;
        if(list1.size() == list2.size()){
        	for(int i=0; i<list1.size(); i++) {
                sum = sum + Math.pow((list1.get(i) - list2.get(i)), 2.0);
             }
        }else if(list1.size() > list2.size()){
        	for(int i=0;i<list1.size();i++) {
        		if(list2.size()>i){
        			sum = sum + Math.pow( (list1.get(i) - list2.get(i)), 2.0);
        		}else{
        			sum = sum + Math.pow( (list1.get(i) - 0), 2.0);
        		}
             }
        }else{
        	for(int i=0; i<list2.size(); i++) {
        		if(list1.size() > i){
        			sum = sum + Math.pow( (list1.get(i) - list2.get(i)), 2.0);
        		}else{
        			sum = sum + Math.pow(0 - (list2.get(i)), 2.0);
        		}             
        	}
        }
        return Math.sqrt(sum);
    }
	
	public static HashMap<String, List<Double>> buildVector(String[] in){
		 HashMap<String, List<Double>> mapWordListScore = new HashMap<String, List<Double>>();
		 String key = in[0];
		 List<Double> list = new LinkedList<Double>();
		 String[] values = in[1].split(",");
		 for(int i=0; i<values.length; i++){
			 String[] arrayWordScore = values[i].split(":");
			 double score = Double.parseDouble(arrayWordScore[1]);
			 list.add(score);
		 }
		 mapWordListScore.put(key, list);
		 return mapWordListScore;
	}
	
	public static HashMap<String, String> buildMap(BufferedReader reader) throws IOException{
		HashMap<String, String> map = new HashMap<String, String>();
		while(true){
			String s = reader.readLine();
			if(s == null) break;
			String [] array = s.split("\t");
			String key = array[0];
			String value = array[1];
			map.put(key, value);
		}
		reader.close();
		return map;
	}
	
	public static boolean compareFiles(BufferedReader reader1, BufferedReader reader2) throws IOException{
		while(true){
			String s1 = reader1.readLine();
			String s2 = reader2.readLine();
			if(s1==null) break;
			if(!s1.equals(s2)) return false;
		}
		return true;
		
	}
	
	public static void main(String[] args) throws Exception {
	 	    
			boolean onlyStep3 = false;
			if(!onlyStep3){
			
				HashMap<String, String> map1 = new HashMap<String, String>();
				HashMap<String, String> map2 = new HashMap<String, String>();
				Configuration conf = new Configuration(true);
				conf.set("K", args[0]);
				FileSystem fs = FileSystem.get(conf);
				FSDataInputStream in1 = null;
				FSDataInputStream in2 = null;
				boolean converge = false;
			
				fs.mkdirs(new Path(copyDir));
	        
				Job job1 = Job.getInstance(conf);
	   
				job1.setMapOutputKeyClass(IntWritable.class);
				job1.setMapOutputValueClass(Text.class);
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(Text.class);
		
				job1.setMapperClass(MapperRandom.class);
				job1.setReducerClass(ReducerRandom.class); 
	 
				job1.setInputFormatClass(TextInputFormat.class);
				job1.setOutputFormatClass(TextOutputFormat.class);
				FileInputFormat.setInputPaths(job1, new Path(args[1]));
				FileOutputFormat.setOutputPath(job1, new Path(centerDir));
	        
				job1.setJarByClass(KMeans.class);
				job1.waitForCompletion(true);
	        
				int iter=0;
			
				do{
					fs.delete(new Path(copyDir), true);
					fs.mkdirs(new Path(copyDir));
					FileUtil.copy(fs, new Path(centerDir+"/part-r-00000"), fs, new Path(copyDir), false, true, conf);
					fs.delete(new Path(centerDir), true);
				
					Job job2 = Job.getInstance(new Configuration());
					job2.setMapOutputKeyClass(Text.class);
					job2.setMapOutputValueClass(Text.class);
					job2.setOutputKeyClass(Text.class);
					job2.setOutputValueClass(Text.class);
	 	 
					job2.setMapperClass(MapperClosestCenter.class);
					job2.setReducerClass(ReducerCentroid.class); 
	 	 
					job2.setInputFormatClass(TextInputFormat.class);
					job2.setOutputFormatClass(TextOutputFormat.class);

					FileInputFormat.setInputPaths(job2, new Path(args[1]));
					FileOutputFormat.setOutputPath(job2, new Path(centerDir));
	 	        
					job2.setJarByClass(KMeans.class);
					job2.waitForCompletion(true);
				
					in1 = fs.open(new Path(copyDir+"/part-r-00000"));
					BufferedReader br = new BufferedReader(new InputStreamReader(in1));
					map1 = buildMap(br);
					br.close();
				
					in2 = fs.open(new Path(centerDir+"/part-r-00000"));
					BufferedReader br1 = new BufferedReader(new InputStreamReader(in2));
					map2 = buildMap(br1);
					br1.close();
	 	        
					converge = map1.equals(map2);
					iter++;
	 	        
				}while(!converge);
			}
	        
	        Job job3 = Job.getInstance(new Configuration());
	        job3.setMapOutputKeyClass(Text.class);
	        job3.setMapOutputValueClass(Text.class);
	        job3.setOutputKeyClass(Text.class);
	        job3.setOutputValueClass(Text.class);
	 
	        job3.setMapperClass(MapperClosestCenter.class);
	        job3.setReducerClass(ReducerClusterVisualization.class); 
	 
	        job3.setInputFormatClass(TextInputFormat.class);
	        job3.setOutputFormatClass(TextOutputFormat.class);
	        FileInputFormat.setInputPaths(job3, new Path(args[1]));
	        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
	        
	        job3.setJarByClass(KMeans.class);
	        job3.waitForCompletion(true);
	        
	    }
 }        
