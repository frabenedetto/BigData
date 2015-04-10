package project1.ex2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;



public class Main2 {

	public static void main( String[] args )
    {
    	
    	JobConf conf1 = new JobConf(Main2.class);
    	conf1.setJobName("SecondExercise_job1");   
    	
    	FileInputFormat.addInputPath(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
        
        conf1.setMapperClass(Job1Mapper.class);
        conf1.setReducerClass(Job1Reducer.class);
        
        conf1.setMapOutputKeyClass(Text.class);
        conf1.setMapOutputValueClass(Text.class);
        
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
        
        try {
			JobClient.runJob(conf1);
		} catch (IOException e) {
			e.printStackTrace();
		}
}
	
}
