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
				DataBag outerBag = mBagFactory.newDefaultBag();
				
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
					for(String s : c){
						DataBag innerB = mBagFactory.newDefaultBag();
						Tuple t = mTupleFactory.newTuple((String) s);
						innerB.add(t);
					}
					outerBag.add(arg0);
				}

			return outerBag;
		} catch(Exception ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}
	public Schema outputSchema(Schema input) {
		try{
			Schema elementSchema = new Schema(new FieldSchema("e", DataType.CHARARRAY)); //definisco il tipo
			
			FieldSchema tupleSchema = new FieldSchema("t", elementSchema, DataType.TUPLE);
			
			Schema bagFieldSchemaT = new Schema(tupleSchema);

			FieldSchema innerBagFieldSchema = new FieldSchema("ib", bagFieldSchemaT, DataType.BAG); //field della bag costruito come tupla sulla base dello schema precedente

			Schema outerBagSchema = new Schema(innerBagFieldSchema);
			
			FieldSchema innerBagWrapper = new FieldSchema("it", )

			FieldSchema bagFieldSchemaB = new FieldSchema("ob", outerBagSchema, DataType.BAG); //field dello schema finale da mandare in output (1 campo, di tipo bag)

			return new Schema(bagFieldSchemaB);
		}catch (Exception e){
			return null;
		}
	}
}
