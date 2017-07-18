package com.mci.mdpd.hive.udfs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;

public class MY_TODATE extends GenericUDF {
	
	private StringObjectInspector inDateTime;
	private StringObjectInspector reqDateFmt;
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		String inDate = inDateTime.getPrimitiveJavaObject(arguments[0].get());
		String targetFormat = reqDateFmt.getPrimitiveJavaObject(arguments[1].get());
		if ( targetFormat == null || targetFormat.length() == 0){
			throw new HiveException(
					"Desired output format must not be empty. Provide a valid format string");
		}
		
		String outDateTime = (inDate == null || inDate.isEmpty() || inDate.length() == 0 )?null:(String) isValidDate(inDate, targetFormat);
		
		System.out.println("Returning Output: " + outDateTime);
		
		return outDateTime;
		
		
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "evaluate if passed string is a valid date or date/time";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 2)
			throw new UDFArgumentLengthException("MY_TODATE UDF takes exactly 2 String Arguments.");
		
		ObjectInspector a =arguments[0];
		ObjectInspector b =arguments[1];
		
		if (!(a instanceof StringObjectInspector) || ! (b instanceof StringObjectInspector) ) {
			throw new UDFArgumentException(
					"Arguments must be of type string");
		}
		
		this.inDateTime = (StringObjectInspector) a;
		this.reqDateFmt = (StringObjectInspector) b;
		
		
		
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		
		
	}
	
	public static Object isValidDate(String inDate,String tgtFormat) {
		String[] vaildDateFormats = {"yyyy-MM-dd'T'HH:mm:ss","yyyy-MM-dd hh:mm:ss a",
									"MM/dd/yyyy HH:mm:ss","yyyy-MM-dd HH:mm:ss","yyyyMMddHHmmss",
									"yyyyMMdd","dd-MMM-yy","yyyy-MM-dd","yyyy/MM/dd","MM/dd/yyyy",
									"dd.MM.yyyy"
									};
		
		
		Object outDate = null;
		for (String fmt:vaildDateFormats)
		{
			SimpleDateFormat dateFormat = new SimpleDateFormat(fmt);
			dateFormat.setLenient(false);
			
			try {
			      
				  Date parsedDate = dateFormat.parse(inDate.trim());
			      
			      System.out.println("Valid Date Checked: " + parsedDate.toString());
			      SimpleDateFormat outputFmt = new SimpleDateFormat(tgtFormat);
			      outDate =  outputFmt.format(parsedDate);
			      break;
			      
			      
			    } catch (ParseException pe) {
			    continue;
			    }
		}
	    
		 
		return outDate;

	    
	  }
	
}
