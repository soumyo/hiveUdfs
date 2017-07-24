package com.mci.hive.udfs;

/** 
 * 
 * Author : Soumyo Das
 * Last Modified : 14th July,2017
 * Description: This is a Hive UDF. It takes 3 args. 
 * 				1st Argument - A Map with Integer Keys only.
 * 				2nd Argument - String - values [merge/nomerge] : Repeating values will be merged to one.
 * 				3rd Argument - String - Values [asc/desc] - Represents sort Order. Default Ascending.
 * 				The UDF sorts the Map keys in the order provided. And builds one string of their values in order of sorted keys.
 * 				Values are separated by '-'.
 *  
 * Version : 1.0
 * Invocation : Hive
 * Inputs: Map (Keys are Integers), [asc/desc] (Default: asc)
 * Output : String 
 * Example : select id,MAP_TO_KEY_SRTD_STR({0:"A",2:"C",1:"B",4:"D",3:"D"},'merge','asc') as path from a;
 * 	O/p: A-B-C-D					  
 * 		     
**/


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import java.util.Map.Entry;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.TreeMap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;

public class MAP_TO_KEY_SRTD_STR extends GenericUDF{
	
	private MapObjectInspector inMapInspector;
	private StringObjectInspector inMergeFlag;
	private StringObjectInspector inSortOrder;

	@SuppressWarnings("unchecked")
	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		
		String outString = "";
		
		HashMap<LazyInteger,LazyString> inputMap = new HashMap<LazyInteger,LazyString>();
		inputMap =  (HashMap<LazyInteger, LazyString>) inMapInspector.getMap(args[0].get());
		String mergeOrNot = inMergeFlag.getPrimitiveJavaObject(args[1].get());
		String sortOrder = inSortOrder.getPrimitiveJavaObject(args[2].get());
		
		if ( !(mergeOrNot.toLowerCase().equals("merge")) && 
				!(mergeOrNot.toLowerCase().equals("nomerge")) ){
			throw new HiveException("2nd Argument must be one of [merge/nomerge]");
		}
		
		if ( !(sortOrder.toLowerCase().equals("asc")) && 
				!(sortOrder.toLowerCase().equals("desc")) ){
			throw new HiveException("3rd Argument must be one of [asc/desc]");
		}
		
		TreeMap<Integer,String> inputMapSrtd = new TreeMap<Integer,String>(); 
		//TreeMap<Integer,String> inputMapSrtdDesc = new TreeMap<Integer,String>(Collections.reverseOrder());
		
		
		if (inputMap.size() > 0) {
			for (Entry<LazyInteger, LazyString> entry : inputMap.entrySet() ){
				String currKey = entry.getKey().toString();
				String value = entry.getValue().getWritableObject().toString();
				inputMapSrtd.put(Integer.parseInt(currKey),value);	
			}
			outString = mapToString(inputMapSrtd,mergeOrNot,sortOrder);
		}
		
		//inputMapSrtdDesc.putAll(inputMapSrtdAsc);
		
		
		return outString;
	}
	
	private static String mapToString(TreeMap<Integer,String> inputMap,String mergeFlag,String order){
		
		LinkedHashSet<Integer> keysSet = new LinkedHashSet<Integer>();
		Deque<Integer> seenKeys = new ArrayDeque<Integer>();
		String outString = "";
		
		if (order.toLowerCase().equals("desc"))
		{
			keysSet.addAll(inputMap.descendingKeySet());
			
		}
		else{
			keysSet.addAll(inputMap.keySet());
		}
		
		Iterator<Integer> keyIter = keysSet.iterator();
		
		while(keyIter.hasNext()){
			String lastVal = "";
			Integer currKey = keyIter.next();
			if (!(seenKeys.isEmpty())){
				Integer lastKey = seenKeys.peek();
				lastVal = inputMap.get(lastKey);
			}
			
			String currVal = inputMap.get(currKey);	
			
			if(mergeFlag.toLowerCase().equals("merge") && currVal.equals(lastVal))
			{
				continue;
			}
			
			outString = (outString.length()==0)?currVal:(outString + "-" + currVal);
			seenKeys.push(currKey);
		}
		
		return outString;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		
		return "UDF returns values from the Map as a string sorted by(asc/desc is an input) keyset "
				+ "and each value separated by -";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		
		if (arg0.length != 3)
			throw new UDFArgumentLengthException(" MAP_TO_KEY_SRTD_STR UDF takes exactly 3 arguments"
					+ "1st- Type:MAP with integer keys"
					+ "2nd- Type:String, options: merge/nomerge"
					+ "3rd- Type:String, options: asc/desc"
					);
		
		ObjectInspector a =arg0[0];
		ObjectInspector b= arg0[1];
		ObjectInspector c= arg0[2];
		
		
		if (!(a instanceof MapObjectInspector) ) {
			throw new UDFArgumentException(
					" 1st Argument must be of type Map with Integer Keys");
		}
		if (!(b instanceof StringObjectInspector) || ! (c instanceof StringObjectInspector) ) {
			throw new UDFArgumentException(
					"2nd/3rd Arguments must be of type String");
		}
		
		this.inMapInspector = (MapObjectInspector) a;
		this.inMergeFlag = (StringObjectInspector) b;
		this.inSortOrder = (StringObjectInspector) c;
		
		
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		
	}

}
