package project1.udf;

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import project1.facA.Product2QuantityPair;

public class TestProduct2QuantityPair {

	public static void main(String[] args) {
		Product2QuantityPair pair = new Product2QuantityPair(new Text("prova"), new IntWritable(1)); 
		ArrayList<Product2QuantityPair> pairs = new ArrayList<Product2QuantityPair>();
		pairs.add(pair);
		Product2QuantityPair toremove = new Product2QuantityPair();
		toremove.setProduct(new Text("prova"));
		Product2QuantityPair result = pairs.remove(pairs.indexOf(toremove));
		System.out.println(result.toString());
		
	}
}
