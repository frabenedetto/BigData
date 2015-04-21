package project1.hive.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "pairwise", value = "_FUNC_(doc) - emits pairwise combinations of a ticket input array")
public class ProductSetsUDTF extends GenericUDTF {

	 private PrimitiveObjectInspector stringOI = null;
	
	@Override

	  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException 

	{
	    if (args.length != 1) {
	      throw new UDFArgumentException("pairwise() takes exactly one argument");
	    }
	 
	    if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE

	        && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != 

	        PrimitiveObjectInspector.PrimitiveCategory.STRING) {
	      throw new UDFArgumentException("pairwise() takes a string as a parameter");
	    }
	 

	 stringOI = (PrimitiveObjectInspector) args[0];
	
	
	List<String> fieldNames = new ArrayList<String>(1);//era 2
    List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);//era 2
    fieldNames.add("memberA");
    //fieldNames.add("memberB");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    //fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

}
	
	
	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}


	@Override
	  public void process(Object[] record) throws HiveException {
		
		
	    final String document = (String) stringOI.getPrimitiveJavaObject(record[0]);
	 
	    if (document == null) {
	      return;
	    }
	    
	    String[] members = document.split(",");
	    
	    
	    
		HashSet<HashSet<String>> coppie = null, triple = null, quadruple = null;
		
	    HashSet<HashSet<String>> singoli = new HashSet<HashSet<String>>();
	    
	    for(String s : members){
	    	if(s.matches("-")) continue;
	    	HashSet<String> tmp = new HashSet<String>();
	    	tmp.add(s);
	    }

		
		if(members.length > 1){
			
			coppie = buildSet(singoli, singoli, 2);
			
			if(members.length > 2){
				
				triple = buildSet(singoli, coppie, 3);
				
				if(members.length > 3){
					
					quadruple = buildSet(singoli, triple, 4);
					
				}
				
			}
			
		}
		
		if(coppie != null)
		for(HashSet<String> ele: coppie){
			forward(new Object[] {buildString(ele)});
		}
		

		if(triple != null)
		for(HashSet<String> ele: triple){
			forward(new Object[] {buildString(ele)});
		}
			
		if(quadruple != null)
		for(HashSet<String> ele: quadruple){
			forward(new Object[] {buildString(ele)});
		}
			
		
	} 

	
	private HashSet<HashSet<String>> buildSet(HashSet<HashSet<String>> singoliElementi, HashSet<HashSet<String>> xElementi, int x ){
		
		HashSet<HashSet<String>> xPiuUnoElementi = 	new	HashSet<HashSet<String>>();
		
		for(HashSet<String>  s: singoliElementi){
			for(HashSet<String> t : xElementi){
				HashSet<String> tmp = new HashSet<String>();
				tmp.addAll(s);
				tmp.addAll(t);
				if(tmp.size() == x)
					xPiuUnoElementi.add(tmp);
			}
		}
		
		
		return xPiuUnoElementi;
		
		
	}
	
	private String buildString(HashSet<String> e){
		StringBuilder ret = new StringBuilder();
		boolean first = true;
		ArrayList<String> tmp = new ArrayList<String>(e);
		Collections.sort(tmp);

		for(String el : tmp){
			if(!first)
				ret.append(",");
			ret.append(el);
			first = false;
			}
		
		
		return ret.toString();
		
		
	}
	
}