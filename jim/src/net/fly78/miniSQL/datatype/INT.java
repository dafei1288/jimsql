package net.fly78.miniSQL.datatype;

public class INT implements DataType{
	private int i;
	
	public INT(){
		i=0;
	}
	public INT(int i){
		this.i = i;
	}
	public INT(String i){
		this.i = Integer.parseInt(i);
	}
	
	public INT(float i){
		this.i = (int)i;
	}
	
	@Override
	public int getLength() {
		return (i+"").length();
	}
	@Override
	public String getStringValue() {
		return i+"";
	}
	@Override
	public Class<?> getType() {
		return INT.class;
	}
	@Override
	public DataType getValue() {
		return this;
	}
	
	@Override
	public int compare(String r) {
		int tag = 0;
		try{
			int fa = Integer.parseInt(r.trim());
			if(fa==this.i){
				tag = 0;
			}
			if(fa<this.i){
				tag = 1;
			}
			if(fa>this.i){
				tag = -1;
			}
			
		}catch(Exception e){
			
		}
		return tag;
	}
	@Override
	public Object getJValue() {
		return this.i;
	}
}
