package project1.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

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

public class CreaGruppi extends EvalFunc<DataBag>{
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();
	StoreCaster converter = new Utf8StorageConverter();
	@Override
	public DataBag exec(Tuple input) throws IOException {

			if (input == null || input.size() == 0)
				return null;
			try {
				DataBag bag = mBagFactory.newDefaultBag();
				
				ArrayList<String> prodotti = new ArrayList<String>();
				
				for(Object o: input.getAll()){
					prodotti.add(new String(converter.toBytes((DataByteArray) o), "UTF-8"));
				}
				
				HashSet<HashSet<String>> coppie = new HashSet<HashSet<String>>();
				HashSet<HashSet<String>> triple = new HashSet<HashSet<String>>();
				HashSet<HashSet<String>> quadruple = new HashSet<HashSet<String>>();
				
				if(prodotti.size() > 1){
					
					for(String s : prodotti){
						for(String t : prodotti){
							HashSet<String> tmp = new HashSet<String>();
							tmp.add(s);
							tmp.add(t);
							if(tmp.size() == 2)
								coppie.add(tmp);
						}
					}
					
				}
				
				if(prodotti.size() > 2){
					
					for(String s : prodotti){
						for(HashSet<String> t : coppie){
							HashSet<String> tmp = new HashSet<String>();
							tmp.add(s);
							tmp.addAll(t);
							if(tmp.size() == 3)
								triple.add(tmp);
						}
					}
				}
				
				if(prodotti.size() > 3){
					
					for(String s : prodotti){
						for(HashSet<String> t : triple){
							HashSet<String> tmp = new HashSet<String>();
							tmp.add(s);
							tmp.addAll(t);
							if(tmp.size() == 4)
								quadruple.add(tmp);
						}
					}
					
				}
				
				for(HashSet<String> c: coppie){
					Tuple t = mTupleFactory.newTuple(2);
					int index = 0;
					for(String s : c){
						t.set(index, s);
						index++;
					}
					bag.add(t);		
				}
				
				for(HashSet<String> c: triple){
					Tuple t = mTupleFactory.newTuple(3);
					int index = 0;
					for(String s : c){
						t.set(index, s);
						index++;
					}
					bag.add(t);		
				}
				
				for(HashSet<String> c: quadruple){
					Tuple t = mTupleFactory.newTuple(4);
					int index = 0;
					for(String s : c){
						t.set(index, s);
						index++;
					}
					bag.add(t);		
				}

			return bag;
			
		} catch(Exception ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}
	public Schema outputSchema(Schema input) {
		try{
			
			FieldSchema tupleSchema = new FieldSchema("t", new Schema(), DataType.TUPLE);
			
			Schema bagSchema = new Schema(tupleSchema);
			
			FieldSchema bagFieldSchema = new FieldSchema("b", bagSchema, DataType.BAG);
			
			return new Schema(bagFieldSchema);
			
			

		}catch (Exception e){
			return null;
		}
	}
}
