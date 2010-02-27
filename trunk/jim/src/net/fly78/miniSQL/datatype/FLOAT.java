package net.fly78.miniSQL.datatype;

public class FLOAT implements DataType{
	private float f ;
	
	public FLOAT(){
		f=0f;
	}
	public FLOAT(int f){
		this.f = Float.parseFloat(f+"");
	}
	public FLOAT(String f){
		this.f = Float.parseFloat(f);
	}
	
	public FLOAT(float f){
		this.f = f;
	}
	
	
	@Override
	public int getLength() {
		return (this.f+"").length();
	}

	@Override
	public String getStringValue() {
		return this.f+"";
	}
	@Override
	public Class<?> getType() {
		return FLOAT.class;
	}
	
	@Override
	public DataType getValue() {
		return this;
	}
	@Override
	public int compare(String r) {
		int tag = 0;
		try{
			float fa = Float.parseFloat(r.trim());
			if(fa==this.f){
				tag = 0;
			}
			if(fa<this.f){
				tag = 1;
			}
			if(fa>this.f){
				tag = -1;
			}
			
		}catch(Exception e){
			
		}
		return tag;
	}
	@Override
	public Object getJValue() {
		return this.f;
	}
}
