package project1.ex2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Job1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
			
		Text product = new Text();
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
		//the first token is the date
		String dateString = tokenizer.nextToken();
		//verify if is in the first 2015 trimester
		boolean validDate = isFirstTrimester2015(dateString);
		//parse date(delete day field)
		if(validDate){
			dateString = dateString.substring(0, 6);
			Text date = new Text(dateString);
		
			while(tokenizer.hasMoreTokens()){
				product.set(tokenizer.nextToken());
				arg2.collect(product, date);
			}
		}
	}
	
	private boolean isFirstTrimester2015(String date) {
		SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd");
		String firstDay = "2014-12-31";
		String lastDay = "2015-04-01";
		Date firstDate=null;
		Date lastDate = null;
		Date toVerify = null;
		try {
			 firstDate = myFormat.parse(firstDay);
			 lastDate = myFormat.parse(lastDay);
			 toVerify = myFormat.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return  toVerify.after(firstDate) && toVerify.before(lastDate);
		
	}
	
	
}
