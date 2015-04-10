package project1.ex1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


public class Main 
{
    public static void main( String[] args )
    {
    	Path temp = new Path("temp");
    	
    	JobConf conf1 = new JobConf(Main.class);
    	conf1.setJobName("FirstExercise_job1");   
    	
    	FileInputFormat.addInputPath(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, temp);
        
        conf1.setMapperClass(Job1Mapper.class);
        conf1.setReducerClass(Job1Reducer.class);
        
        conf1.setMapOutputKeyClass(Text.class);
        conf1.setMapOutputValueClass(IntWritable.class);
        
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(IntWritable.class);
        
        try {
			JobClient.runJob(conf1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        JobConf conf2 = new JobConf(Main.class);
        conf2.setJobName("FirstExercise_job2");
        
        FileInputFormat.addInputPath(conf2, temp);
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
        
        conf2.setMapperClass(Job2Mapper.class);
        conf2.setReducerClass(Job2Reducer.class);
        
        conf2.setMapOutputKeyClass(IntWritable.class);
        conf2.setMapOutputValueClass(Text.class);
        
        conf2.setOutputKeyClass(IntWritable.class);
        conf2.setOutputKeyComparatorClass(DescendingOrderComparator.class);
        conf2.setOutputValueClass(Text.class);
        
        try {
			JobClient.runJob(conf2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    	

    }
}
