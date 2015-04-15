package project1.facA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Product2QuantityPair, Text, IntWritable>{

		
		
		public void reduce(Text arg0, Iterable<Product2QuantityPair> arg1,
				Context ctx)
				throws IOException, InterruptedException {
			
			ArrayList<Product2QuantityPair> pairs = new ArrayList<Product2QuantityPair>();
			for(Product2QuantityPair pair: arg1){
				pairs.add(pair);
			}
			
			/*get the product-quantity pair of the key(that is a product)*/
			Product2QuantityPair toRemove = new Product2QuantityPair();
			toRemove.setProduct(arg0);
			Product2QuantityPair pairOfKey = pairs.remove(pairs.indexOf(toRemove));
			//num of the final fraction
			double num =  pairOfKey.getQuantity().get();
			
		
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
