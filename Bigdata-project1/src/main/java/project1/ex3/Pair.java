package project1.ex3;

import org.apache.hadoop.io.IntWritable;

public class Pair{

	private ProductPair pp;
	private IntWritable q;
	
	public Pair(){}
	
	public Pair(ProductPair pp, IntWritable q) {
		this.pp = pp;
		this.q=q;
		}

	public ProductPair getPp() {
		return pp;
	}

	public void setPp(ProductPair pp) {
		this.pp = pp;
	}

	public IntWritable getQ() {
		return q;
	}

	public void setQ(IntWritable q) {
		this.q = q;
	}

	@Override
	public int hashCode() {
		return pp.hashcode()+q.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Pair) {
            	Pair pair = (Pair) o;
            return pp.equals(pair.getPp())
                    && q.equals(pair.getQ());
        }
        return false;
	}

	/*public int compareTo(Object o) {
		Pair arg0 = (Pair) o;
		int cmp = this.q.compareTo(arg0.getQ());
		if(cmp==0){
			return this.pp.compareTo(arg0.getPp());
		}
		return cmp;
	}
	*/

	
	
	
	
}
