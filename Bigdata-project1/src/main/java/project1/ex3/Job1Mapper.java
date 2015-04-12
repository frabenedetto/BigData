package project1.ex3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job1Mapper extends Mapper<LongWritable, Text, ProductPair, IntWritable>{

	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable arg0, Text value,
			Context ctx) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
		//date not needed
		tokenizer.nextToken();
		//store products in an arrayList
		ArrayList<String> tokens = new ArrayList<String>();
		while(tokenizer.hasMoreTokens()){
			tokens.add(tokenizer.nextToken());
			}
		//generate the productPairs and insert them in the context
		for(int i=0;i<tokens.size()-1;i++){
			for(int j=i+1;j<tokens.size();j++){
				String p1 = tokens.get(i);
				String p2 = tokens.get(j);
				ProductPair pp = null;
				int cmp = p1.compareTo(p2);
				if(cmp<0){
				   pp = new ProductPair(new Text(p1), new Text(p2));
				}
				else {
				   pp = new ProductPair(new Text(p2), new Text(p1));
				}
				ctx.write(pp, one);
			}
		}
	}

}
