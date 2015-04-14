package project1.udf;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Accoppia extends EvalFunc<Tuple> {
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			Tuple t = TupleFactory.getInstance().newTuple();
			for(int i=0; i<input.size(); i++){
				for (int j=i+1; j<input.size(); j++){
					Tuple tmp = TupleFactory.getInstance().newTuple(2);
					tmp.set(0, input.get(i));
					tmp.set(1, input.get(j));
					t.append(tmp);
				}
			}
			return t;
		} catch(Exception ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}
}