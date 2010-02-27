package net.fly78.miniSQL.datatype;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DATE implements DataType{
	private Date date;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
	private String datestr;
	
	public DATE(){
		this.date=new Date();
		datestr = this.sdf.format(this.date);
	}
	public DATE(Date d){
		this.date = d;
		datestr = this.sdf.format(this.date);
	}
	public DATE(String i){
		//this.date =
		
		try {
			this.date =sdf.parse(i);
			datestr = i;
		} catch (ParseException e) {
			this.date = new Date();
		}
		
	}
	
	@Override
	public int getLength() {
		return this.datestr.length();
	}

	@Override
	public String getStringValue() {
		return this.datestr;
	}

	@Override
	public Class<?> getType() {
		return DATE.class;
	}
	@Override
	public DataType getValue() {
		return this;
	}
	@Override
	public int compare(String r) {
		return 0;
	}
	@Override
	public Object getJValue() {
		return this.date;
	}

}
