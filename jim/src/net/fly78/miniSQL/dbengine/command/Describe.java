package net.fly78.miniSQL.dbengine.command;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import net.fly78.miniSQL.dbengine.Connection;

import org.apache.commons.lang.StringUtils;

public class Describe  extends CommandAdpter {
	public static final String DESCRIBE = "describe"; 
	@Override
	public void excuse(String cmd) {
		this.cmdstart = new Date();
		String tablename = StringUtils.trim(StringUtils.replace(cmd, DESCRIBE, ""));
		try {
			BufferedReader br = new BufferedReader(new FileReader(Connection.getInstance().getTable(tablename)));
			
			String init_table = br.readLine();
			int cols = Integer.parseInt(init_table.split(" ")[0]);
			for(int i=0;i<cols;i++){
				String tabled = br.readLine();
				
				System.out.println(tabled);
			}
			this.cmdend = new Date();
			System.out.println( String.format("( time: %.2f sec)", (float)(this.cmdend.getTime()-this.cmdstart.getTime())/1000)  );
		} catch (FileNotFoundException e) {
			logger.error(e.toString());
			logger.error("unknow table");
			System.out.println("unknow table");
		} catch (IOException e) {
			logger.error(e.toString());
			logger.error("read table error");
			System.out.println("read table error");
		} catch (Exception e) {
			logger.error(e.toString());
			logger.error("runtime error please retry");
			System.out.println("runtime error please retry");
		}
	}

}
