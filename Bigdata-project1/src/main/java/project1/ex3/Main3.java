package project1.ex3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main3 extends Configured implements Tool{

	public int run(String[] args) throws Exception {
	    Path input = new Path(args[0]);
	    Path temp1 = new Path("temp1");
	    Path output = new Path(args[1]);
	    Configuration conf = getConf();
	    
	    Job job1 = new Job(conf, "top-k-pass-1");
	    FileInputFormat.addInputPath(job1, input);
	    FileOutputFormat.setOutputPath(job1, temp1);
	    
	    job1.setJarByClass(Main3.class);
	    
	    job1.setMapperClass(Job1Mapper.class);
	    job1.setReducerClass(Job1Reducer.class);
	    
	    job1.setInputFormatClass(TextInputFormat.class);
	    
	    job1.setMapOutputKeyClass(ProductPair.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    boolean succ = job1.waitForCompletion(true);
	    if (! succ) {
	      System.out.println("Job1 failed, exiting");
	      return -1;
	    }
	
	    Job job2 = new Job(conf, "top-k-pass-2");
	    FileInputFormat.setInputPaths(job2, temp1);
	    FileOutputFormat.setOutputPath(job2, output);
	    
	    job2.setJarByClass(Main3.class);
	    
	    job2.setMapperClass(Job2Mapper.class);
	    job2.setReducerClass(Job2Reducer.class);
	    
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setMapOutputKeyClass(ProductPair.class);
	    job2.setMapOutputValueClass(IntWritable.class);
	    
	    job2.setNumReduceTasks(1);
	    succ = job2.waitForCompletion(true);
	    if (! succ) {
	      System.out.println("Job2 failed, exiting");
	      return -1;
	    }
	    
	    return 0;

	  }

	  public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.exit(-1);
	    }
	    int res = ToolRunner.run(new Configuration(), new Main3(), args);
	    System.exit(res);
	  }
	
}
