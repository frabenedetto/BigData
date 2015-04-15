package project1.facA;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*mappa da (a,b -> q) in a -> ([b,q]) e b -> ([a,q])*/
public class Job2Mapper extends Mapper<LongWritable, Text, Text, Product2QuantityPair>{

	
	public void map(LongWritable arg0, Text value,
			Context ctx) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while(tokenizer.hasMoreTokens()){
			Text productLeft = new Text(tokenizer.nextToken());
			Text productRight = new Text(tokenizer.nextToken());
			IntWritable quantity = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
			
			//generate first key->value
			Product2QuantityPair p1 = new Product2QuantityPair(productRight, quantity);
			ctx.write(productLeft, p1);
			//if the couple is (a,a)->q don't insert it again, while if is (a,b -> q) write the b -> [a, q] in the context
			if(!productLeft.equals(productRight)){
				//generate second key->value
				Product2QuantityPair p2 = new Product2QuantityPair(productLeft, quantity);
				ctx.write(productRight, p2);
				}
		}
		}
	
	
	
}
