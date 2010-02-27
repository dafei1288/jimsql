package net.fly78.miniSQL.datatype;

public class STR implements DataType{
	
	private String str;
	
	public STR(){
		str="";
	}
	public STR(int i){
		str =i+"";
	}
	public STR(String i){
		this.str = i;
	}
	public STR(float i){
		str =i+"";
	}
	@Override
	public int getLength() {
		return this.str.length();
	}

	@Override
	public String getStringValue() {
		return this.str;
	}
	
	@Override
	public Class<?> getType() {
		return STR.class;
	}
	
	@Override
	public DataType getValue() {
		return this;
	}
	
	@Override
	public int compare(String r) {
		int tag = 0;
		try{
			if(this.str.equals(r.trim())){
				tag = 0;
			}else{
				tag = 1;
			}
			
		}catch(Exception e){
			
		}
		return tag;
	}
	@Override
	public Object getJValue() {
		return this.str;
	}
	
}	
