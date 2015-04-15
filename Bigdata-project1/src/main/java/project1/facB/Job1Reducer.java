package project1.facB;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<HashSet<String>, IntWritable, Text, IntWritable>{

	
	
	public void reduce(HashSet<String> arg0, Iterable<IntWritable> arg1,
			Context ctx)
			throws IOException, InterruptedException {

		int sum = 0;
		for(IntWritable value: arg1) {
			sum+=value.get();
		}
		
		ctx.write(new Text(setToString(arg0)), new IntWritable(sum));
		
	}
	
	private String setToString(HashSet<String> hs){
		StringBuilder str= new StringBuilder();
		boolean first = true;
		for(String s: hs){
			if(!first)
				str.append(',');
			str.append(s);
			first = false;
		}
		
		return '(' + str.toString() + ')';
	}

}
