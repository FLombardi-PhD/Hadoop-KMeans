import java.io.*;
import java.util.*; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Data Mining Homework 5 - exercise 3
 * KMeans clustering: compute new centroids
 * @author Federico Lombardi
 */
 
public class ReducerCentroid extends  Reducer<Text,Text, Text, Text>{
	
	public static List<Double> buildVector(String s){
		List<Double> scoreList = new LinkedList<Double>();
		String[] tokens = s.split("\t");
		String[] values = tokens[1].split(" ");
		for(int i=0; i<values.length; i++){
			double score = Double.parseDouble(values[i]);
			scoreList.add(i, score);
		}
		return scoreList;
	}
	
	private Text word = new Text();
	private Text out = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		TreeSet<Integer> tree = new TreeSet<Integer>();
		ArrayList<List<Double>> listOfScoreList = new ArrayList<List<Double>>();
		
		Iterator<Text> itValues = values.iterator();
		String centerId = key.toString();
		int index = 0;
		while(itValues.hasNext()){
			String value = itValues.next().toString();
			List<Double> scoreList = ReducerCentroid.buildVector(value);
			tree.add(scoreList.size());
			listOfScoreList.add(index, scoreList);
			index++;
		}
		
		String centroid = "";
		int N = tree.last();
		
		for (int i=0; i<N; i++){
			double sum=0.0;
			int k=0;
			for (int j=0;j<listOfScoreList.size();j++){
				List<Double> currentScoreList = listOfScoreList.get(j);
				int currentScoreListSize = currentScoreList.size();
				if(currentScoreListSize<=i){
					sum += 0.0;
				}else{
					Double currentScore = currentScoreList.get(i);
					sum += currentScore;
				}
			}
			
			double newC = sum / ((double)listOfScoreList.size());
			centroid += String.valueOf(newC)+" ";
			
		}
		
		word.set(centroid);
		context.write(key, word);
			
	}

}