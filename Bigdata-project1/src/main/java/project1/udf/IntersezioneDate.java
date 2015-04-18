package project1.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.StoreCaster;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class IntersezioneDate extends EvalFunc<Tuple>{
	
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();
	StoreCaster converter = new Utf8StorageConverter();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		
		try {
			Tuple output = mTupleFactory.newTuple(3);
			output.set(0, input.get(0));
			output.set(1, input.get(1));
			double j  = Long.valueOf((Long) input.get(2)).doubleValue();

			
			DataBag bag = (DataBag) input.get(3);
			
			Iterator<Tuple> itBag = bag.iterator();
			
			Tuple t1 = itBag.next();
			Tuple t2 = itBag.next();
				
			DataBag b1 = (DataBag) t1.get(0);
			DataBag b2 = (DataBag) t2.get(0);
			
			ArrayList<String> l1 = new ArrayList<String>();
			ArrayList<String> l2 = new ArrayList<String>();
			
			for(Tuple tmp : b1){
				String i_s = new String(converter.toBytes((DataByteArray) tmp.get(0)), "UTF-8");
				l1.add(i_s);
			}
				

			for(Tuple tmp : b2){
				String i_s = new String(converter.toBytes((DataByteArray) tmp.get(0)), "UTF-8");
				l2.add(i_s);
			}
			
			
			double intersect_size = CollectionUtils.intersection(l1, l2).size();
			
			output.set(2, (intersect_size/j)*100);
			
			return output;
		} catch(Exception ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}
	
	public Schema outputSchema(Schema input) {
		try{
			Schema fieldSchema = new Schema();

			fieldSchema.add(new FieldSchema("p1", DataType.CHARARRAY)); //campo 1 della tupla
			fieldSchema.add(new FieldSchema("p2", DataType.CHARARRAY)); //campo 2 della tupla
			fieldSchema.add(new FieldSchema("freq", DataType.DOUBLE));
			
			Schema tupleSchema = new Schema(fieldSchema);

			return tupleSchema;
			
		}catch (Exception e){
			return null;
		}
	}
}
