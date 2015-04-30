package project1.pig.udf;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.StoreCaster;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.data.DataByteArray;

public class Accoppia extends EvalFunc<DataBag> {
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();
	StoreCaster converter = new Utf8StorageConverter();
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			DataBag output = mBagFactory.newDefaultBag();
			
			if(input.size()>1){

				for(int i=0; i<input.size(); i++){
					String i_s = new String(converter.toBytes((DataByteArray) input.get(i)), "UTF-8");
					for (int j=i+1; j<input.size(); j++){
						String j_s = new String(converter.toBytes((DataByteArray) input.get(j)), "UTF-8");
						Tuple tmp = mTupleFactory.newTuple(2);
						int comp = i_s.compareTo(j_s);
						if(comp<0){
							tmp.set(0, i_s);
							tmp.set(1, j_s);
							}
						else{
							tmp.set(0, j_s);
							tmp.set(1, i_s);
							}
						
					
					output.add(tmp);
					
				}
			}
		}
	

		return output;
	} catch(Exception ee) {
		throw new IOException("Caught exception processing input row ", ee);
	}
}

public Schema outputSchema(Schema input) {
	try{
		Schema fieldSchema = new Schema();

		fieldSchema.add(new FieldSchema("e1", DataType.CHARARRAY)); //campo 1 della tupla
		fieldSchema.add(new FieldSchema("e2", DataType.CHARARRAY)); //campo 2 della tupla

		Schema tupleSchema = new Schema(fieldSchema); //schema della tupla con campi e1 ed 22

		FieldSchema tupleFieldSchema = new FieldSchema("t", tupleSchema, DataType.TUPLE); //field della bag costruito come tupla sulla base dello schema precedente

		Schema bagSchema = new Schema(tupleFieldSchema); //schema della bag con il campo t

		FieldSchema bagFieldSchema = new FieldSchema("b", bagSchema, DataType.BAG); //field dello schema finale da mandare in output (1 campo, di tipo bag)

		return new Schema(bagFieldSchema); //schema con i field definiti in bagFieldSchema
	}catch (Exception e){
		return null;
	}
}
}