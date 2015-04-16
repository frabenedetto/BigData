package project1.facA;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Product2QuantityPair, Text, IntWritable>{

		
		
		public void reduce(Text arg0, Iterable<Product2QuantityPair> arg1,
				Context ctx)
				throws IOException, InterruptedException {
			double num = 0;
			ArrayList<Product2QuantityPair> pairs = new ArrayList<Product2QuantityPair>();
			
			/*aggiungi la coppia solo se il prodotto è diverso da quello della chiave
			 * altrimenti prendine la quantità che verrà usata come numeratore per 
			 * il calcolo del valore della frazione finale*/
			for(Product2QuantityPair pair: arg1){
				Product2QuantityPair ins = new Product2QuantityPair(pair.getProduct(), pair.getQuantity());
				if(!(ins.getProduct().equals(arg0)))
					pairs.add(ins);
				else
					num = ins.getQuantity().get();
			}
			
			
			if(pairs.size()>0){
				//generate all the couples (a, x) -> Qa/Qx
				for(Product2QuantityPair pq: pairs) {
					ProductPair pp = new ProductPair(arg0, pq.getProduct());
					double den = pq.getQuantity().get();
					int fraction = (int)((num/den)*100);
					ctx.write(new Text(pp.toString()), new IntWritable(fraction));
				}
			}
			//if the product is always alone (has never been sold with another product)
			else {
				ctx.write(arg0, new IntWritable(0));
			}
			
			
		}
}
