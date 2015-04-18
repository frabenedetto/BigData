package project1.facB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable arg0, Text value,
			Context ctx) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
		//date not needed
		tokenizer.nextToken();
		
		//raccolgo i token
		List<String> tokens = new ArrayList<String>();
		while(tokenizer.hasMoreTokens()){
			String t = tokenizer.nextToken();
			tokens.add(t);
		}
		
		//array che contiene in 0 le coppie, 1 le triple e 2 le quadruple
		List<HashSet<HashSet<String>>> productSets = new ArrayList<HashSet<HashSet<String>>>(3);
		
		//genero gli insiemi che conterranno le coppie, le triple e le quadruple; uso gli hashset per eliminare eventuali doppi.
		for(int i=0; i<3; i++){
			productSets.add(new HashSet<HashSet<String>>());
		}
		
		//copia dei token per fare il primo accoppiamento
		List<String> tokensCopy = new ArrayList<String>(tokens);
		
		//per ogni token genero insiemi di 2 elementi
		for (String s: tokens){
			for(String t: tokensCopy){
				HashSet<String> prodTmp = new HashSet<String>();
				prodTmp.add(s);
				prodTmp.add(t);
				if(prodTmp.size() == 2)
					productSets.get(0).add(prodTmp);
			}
		}
		
		// per ogni token prendo gli insiemi di due elementi e genero insiemi di tre elementi
		for(String s: tokens){
			for(HashSet<String> set: productSets.get(0)){
				HashSet<String> prodTmp = new HashSet<String>();
				prodTmp.add(s);
				prodTmp.addAll(set);
				if(prodTmp.size() == 3)
					productSets.get(1).add(prodTmp);
			}
		}
		
		for(String s: tokens){
			for(HashSet<String> set: productSets.get(1)){
				HashSet<String> prodTmp = new HashSet<String>();
				prodTmp.add(s);
				prodTmp.addAll(set);
				if(prodTmp.size() == 4)
					productSets.get(2).add(prodTmp);
			}
		}
		
		for(HashSet<HashSet<String>> hs: productSets){
			for(HashSet<String> hs_s: hs){
				ctx.write(new Text(setToString(hs_s)), one);
			}
		}
		
		
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
