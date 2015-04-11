package project1.ex3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Main3 {

	public static void main( String[] args )
    {
    	
    	JobConf conf1 = new JobConf(Main3.class);
    	conf1.setJobName("ThirdExercise_job1");   
    	
    	FileInputFormat.addInputPath(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
        
        conf1.setMapperClass(Job1Mapper.class);
        conf1.setReducerClass(Job1Reducer.class);
        
        conf1.setMapOutputKeyClass(ProductPair.class);
        conf1.setMapOutputValueClass(IntWritable.class);
        
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(IntWritable.class);
        
        try {
			JobClient.runJob(conf1);
		} catch (IOException e) {
			e.printStackTrace();
		}
}
	
}
