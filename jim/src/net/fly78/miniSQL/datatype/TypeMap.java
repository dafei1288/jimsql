package net.fly78.miniSQL.datatype;

import java.util.Hashtable;

public class TypeMap {
	public final static Hashtable<String,Class> HS;
	static{
		HS = new Hashtable<String,Class>();
		HS.put("INT", INT.class);
		HS.put("STR", STR.class);
		HS.put("FLOAT", FLOAT.class);
		HS.put("DATE", DATE.class);
		
		
	}
}
