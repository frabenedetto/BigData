package project1.job1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingOrderComparator extends WritableComparator{
	 protected DescendingOrderComparator() {
	        super(IntWritable.class, true);
	    }   
	
	 @SuppressWarnings("rawtypes")
	@Override
	 public int compare(WritableComparable w1, WritableComparable w2) {
		 IntWritable q1 = (IntWritable)w1;
		 IntWritable q2 = (IntWritable)w2;
		 
		 return q1.compareTo(q2)*-1;
		 }
	

	
	
	
}
