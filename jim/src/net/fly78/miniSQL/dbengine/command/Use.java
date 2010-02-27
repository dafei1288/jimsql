package net.fly78.miniSQL.dbengine.command;

import java.io.File;

import org.apache.commons.lang.StringUtils;

import net.fly78.miniSQL.dbengine.Connection;

public class Use extends CommandAdpter {
	public static final String USE = "use";
	@Override
	public void excuse(String cmd) {
		String dbname = StringUtils.trim(StringUtils.replaceOnce(cmd, USE, ""));
		File tmpt;
		try {
			tmpt = new File(Connection.getInstance().getStorge().getName()+File.separatorChar+dbname);
			if(!tmpt.exists()){
				System.out.println("unknow database");
				return;
			}
			Connection.getInstance().setDatabasefile(new File(Connection.getInstance().getStorge().getName()+File.separatorChar+dbname));
			System.out.println("database changed");
		} catch (Exception e) {
			System.out.println("runtime error please retry");
		}
		
		
		
	
	}
}
