package com.mci.hive.udfs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;


public class FUZZY_MATCH extends GenericUDF{
	private static final String JARO_WINCKLER = "JW";
	private static final String NGRAM = "NG";
	private static final String LEVENSTEIN = "LV";

	private static final StringDistance JW_DIST = new JaroWinklerDistance();
	private static final StringDistance NG_DIST = new NGramDistance();
	private static final StringDistance LV_DIST = new LevensteinDistance();

	private StringObjectInspector string1;
	private StringObjectInspector string2;
	private StringObjectInspector string3;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 3)
			throw new UDFArgumentLengthException("fuzzysearch takes 3 arguments.");
		ObjectInspector a = arguments[0];
		ObjectInspector b = arguments[1];
		ObjectInspector c = arguments[2];
		if (!(a instanceof StringObjectInspector) || !(b instanceof StringObjectInspector)
				|| !(c instanceof StringObjectInspector)) {
			throw new UDFArgumentException(
					"all arguments must be of type string - algo in third position, available : JW, NG or LV");
		}
		this.string1 = (StringObjectInspector) a;
		this.string2 = (StringObjectInspector) b;
		this.string3 = (StringObjectInspector) c;
		
		return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		String str1 = string1.getPrimitiveJavaObject(arguments[0].get());
		String str2 = string2.getPrimitiveJavaObject(arguments[1].get());
		String algo = string3.getPrimitiveJavaObject(arguments[2].get());
		if (str1==null || str1.length()==0 || str2==null || str2.length()==0 || algo==null || algo.length()==0) {
			throw new HiveException(
					"args must not be empty \n - available algos are JW (jaro-winckler), NG (ngram) or LV (levenstein)");
		}
		switch (algo) {
		case JARO_WINCKLER:
			return JW_DIST.getDistance(str1, str2);
		case NGRAM:
			return NG_DIST.getDistance(str1, str2);
		case LEVENSTEIN:
			return LV_DIST.getDistance(str1, str2);
		default:
			return JW_DIST.getDistance(str1, str2);
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return "evaluate match of two string";
	}

}
