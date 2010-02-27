package net.fly78.miniSQL.dbengine.command;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;

import net.fly78.miniSQL.dbengine.Connection;

public class Create extends CommandAdpter {
	public static String CREATE = "create";
	@Override
	public void excuse(String cmd) {
		
		String dbname = StringUtils.trim(StringUtils.replace(cmd, CREATE, ""));
		String methodname = dbname.split(" ")[0];
		String nname = StringUtils.trim(StringUtils.replace(dbname.split(" ")[1], methodname, ""));
		
		
		try {
//			for(Method m : this.getClass().getMethods()){
//				System.out.println(m.getName());
//			}
			Method m = this.getClass().getMethod(methodname,new Class[]{String.class,String.class});
			m.invoke(this,nname,cmd);
		} catch (Exception e) {
			System.out.println("runtime error please retry");
		} 
		
	}
	
	public void database(String dbname,String cmd){
		File tmpt;
		try {
			tmpt = new File(Connection.getInstance().getStorge().getName()+File.separatorChar+dbname);
			if(tmpt.exists()){
				logger.error("database allready exits");
				System.out.println("database allready exits");
				return ;
			}
			
			if(tmpt.mkdir())
				System.out.println("database created");
			else
				System.out.println("database create fail");
		} catch (Exception e) {
			logger.error(e.toString());
			System.out.println("runtime error please retry");
		} 
	}
	
	public void table(String name,String cmd){
		File tmpt = new File("");
		try {
			tmpt = Connection.getInstance().getTable(StringUtils.split(name, "(")[0]);
			if(tmpt.exists()){
				System.out.println("table allready exits");
				return ;
			}
			//create table test(sid INT 8,name STR 10);
			if(tmpt.createNewFile()){
				System.out.println("table created");
				String tablestrs = StringUtils.split(cmd,"(")[1].trim().replace(")","");	
				String[] strarray = tablestrs.split(",");
				
				BufferedWriter bw = new BufferedWriter(new FileWriter(tmpt));
				bw.append(strarray.length+"\n");
				for(String str : strarray){
					bw.append(str+"\n");
				}
				bw.close();
				
			}else
				System.out.println("table create fail");
		} catch (Exception e) {
			e.printStackTrace();
			tmpt.deleteOnExit();
			System.out.println("runtime error please retry");
		} 
	}
	
}
