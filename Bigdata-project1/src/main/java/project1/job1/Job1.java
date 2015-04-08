package project1.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Hello world!
 *
 */
public class Job1 
{
    public static void main( String[] args )
    {
    	Job job1 = new Job(new Configuration(), "ProductCount_Job1");
    	job1.setJarByClass(Job1.class);
    	
    	job1.setMapperClass(Job1Mapper.class);
    	job1.setReducerClass(Job1Reducer.class);
    	
    	FileInputFormat.addInputPath(job1, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    	
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(IntWritable.class);
    	
    	job1.waitForCompletion(true);

    }
}
