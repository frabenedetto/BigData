package project1.ex2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Job1Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	public void reduce(Text arg0, Iterator<Text> arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		
		TreeMap<Text, Integer> date2quantity = new TreeMap<Text, Integer>();
		String dates2quantityString = "";
		
		while(arg1.hasNext()) {
			Text key = new Text(arg1.next().toString());
			if(date2quantity.containsKey(key)){
				Integer incr = date2quantity.get(key)+1;
				date2quantity.put(key, incr);
			}
			else {
			
				date2quantity.put(key, new Integer(1));
			}
		}
		
		for(Text date : date2quantity.keySet()){
			 dates2quantityString = dates2quantityString.concat( date.toString()+" : "+date2quantity.get(date).toString()+"  ");
		}
	
		
		/*
		while(arg1.hasNext()){
			arg2.collect(arg0, arg1.next());
		}
		*/
		/*
		for(Text date : date2quantity.keySet()){
			arg2.collect(arg0, date);
		}
		*/
		arg2.collect(arg0, new Text(dates2quantityString));
		
	}

	
}
