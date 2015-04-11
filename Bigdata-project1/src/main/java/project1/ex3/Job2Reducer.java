package project1.ex3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<ProductPair, IntWritable, Text, IntWritable>{

	private PriorityQueue<Pair> topK;
	
	@Override
    protected void setup(Context ctx) {
      topK = new PriorityQueue<Pair>(10, new Comparator<Pair>() {
          public int compare(Pair p1, Pair p2) {
              return p1.getQ().compareTo(p2.getQ());
            }
          });     
	}

	
	public void reduce(ProductPair arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException {
		
		for(IntWritable value: arg1) {
			IntWritable quantity = new IntWritable(value.get());
			ProductPair ppIns = new ProductPair(arg0.getProductLeft(), arg0.getProductRight());
			Pair pair = new Pair(ppIns, quantity);
			topK.add(pair);
			if(topK.size()>10) {
				topK.remove(); //removes head of the queue(the smallest element here)
			}
		
		}
	}
	
	
		
		@Override
	    protected void cleanup(Context ctx) throws IOException, InterruptedException {
			
			List<Pair> topKPairs = new ArrayList<Pair>();
		      while (! topK.isEmpty()) {
		        topKPairs.add(topK.remove());
		      }
			
			for(int i = topKPairs.size()-1;i>=0;i--){
				Pair topkPair = topKPairs.get(i);
				ctx.write(new Text(topkPair.getPp().toString()), new IntWritable(topkPair.getQ().get()));
			}
		}
		
	}


