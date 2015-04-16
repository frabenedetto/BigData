package project1.facB;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MainB extends Configured implements Tool{

	public int run(String[] args) throws Exception {
	    Path input = new Path(args[0]);
	    Path output = new Path(args[1]);
	    Configuration conf = getConf();
	  
	    Job job1 = new Job(conf, "ex-b");
	    FileInputFormat.addInputPath(job1, input);
	    FileOutputFormat.setOutputPath(job1, output);
	    
	    job1.setJarByClass(MainB.class);
	    
	    
	    job1.setMapperClass(Job1Mapper.class);
	    job1.setReducerClass(Job1Reducer.class);
	    
	    job1.setInputFormatClass(TextInputFormat.class);
	    
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    boolean succ = job1.waitForCompletion(true);
	    if (! succ) {
	      System.out.println("Job1 failed, exiting");
	      return -1;
	    }
	
	    
	    return 0;

	  }

	  public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.exit(-1);
	    }
	    int res = ToolRunner.run(new Configuration(), new MainB(), args);
	    System.exit(res);
	  }
	
}
