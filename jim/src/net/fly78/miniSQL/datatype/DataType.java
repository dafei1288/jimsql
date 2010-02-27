package net.fly78.miniSQL.datatype;

public interface DataType {
	public String getStringValue();
	public int getLength();
	public Class<?> getType();
	public DataType getValue();
	public int compare(String r);
	
	public Object getJValue();
	
}
