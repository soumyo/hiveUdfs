package com.mci.mdpd.hive.udfs;




import java.util.HashMap;
//import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;

public class MAP_MAXKEY_VALUE  extends GenericUDF{
	
	private MapObjectInspector mapInspector;
 

	@SuppressWarnings("unchecked")
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		TreeMap<Integer,String> inputMapSrtd = new TreeMap<Integer,String>(); 
		HashMap<LazyInteger,LazyString> inputMap = new HashMap<LazyInteger,LazyString>();
		inputMap =  (HashMap<LazyInteger, LazyString>) mapInspector.getMap(arg0[0].get());
		if (inputMap.size() > 0) {
		for (Entry<LazyInteger, LazyString> entry : inputMap.entrySet() ){
			
			//System.out.println("Current map KV" + entry.toString());
			 String currKey = entry.getKey().toString();
			 String value = entry.getValue().getWritableObject().toString();
			 inputMapSrtd.put(Integer.parseInt(currKey),value);
			
		}
					
		Entry<Integer, String> minKV = inputMapSrtd.firstEntry();
		Entry<Integer, String> maxKV = inputMapSrtd.lastEntry();
		//System.out.println("Max Key Value:" + maxKV.getValue());
		
		return maxKV.getValue();
	}
		else{
			return null;
		}
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "Find the Value of the Highest Key Item in the Map";
	
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		if (arg0.length != 1)
			throw new UDFArgumentLengthException("MAP_MAXKEY_VALUE UDF takes exactly 1 Map as Argument.");
		
		ObjectInspector a =arg0[0];
		
		if (!(a instanceof MapObjectInspector) ) {
			throw new UDFArgumentException(
					"Arguments must be of type Map ");
		}
		
		this.mapInspector = (MapObjectInspector) a;
		
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

}
