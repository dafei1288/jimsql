package net.fly78.miniSQL.dbengine.command;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import net.fly78.miniSQL.dbengine.Connection;

public class Show extends CommandAdpter {
	
	public static final String SHOW = "show";
	
	@Override
	public void excuse(String cmd) {
		this.cmdstart = new Date();
		String subname = StringUtils.trim(StringUtils.replace(cmd.toLowerCase(), SHOW, ""));
		try {
//			for(Method m : this.getClass().getMethods()){
//				System.out.println(m.getName());
//			}
			
			Method m = this.getClass().getMethod(subname.toLowerCase());
			m.invoke(this);
			this.cmdend = new Date();
			System.out.println( String.format("( time: %.2f sec)", (float)(this.cmdend.getTime()-this.cmdstart.getTime())/1000)  );
			//System.out.println(String.format("sss  %.2f 333", 11f));
		
		} catch (Exception e) {
			logger.error(e.toString());
			System.out.println("runtime error please retry");
		} 
	}
	
	
	public void tables(){
		String[] fl;
		try {
			if(!Connection.getInstance().getDatabasefile().exists())return;
			fl = Connection.getInstance().getDatabasefile().list();
			for(String str : fl){
				System.out.println(str);
			}
		} catch (Exception e) {
			System.out.println("runtime error please retry");
		}
	}
	
	public void database(){
		String[] fl;
		try {
			if(!Connection.getInstance().getStorge().exists())return;
			fl = Connection.getInstance().getStorge().list();
			for(String str : fl){
				System.out.println(str);
			}
		} catch (Exception e) {
			System.out.println("runtime error please retry");
		}
	}
}
