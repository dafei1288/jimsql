package net.fly78.miniSQL.dbengine.command;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import net.fly78.dafei1288.util.string.StringUtil;
import net.fly78.miniSQL.datatype.TypeMap;
import net.fly78.miniSQL.dbengine.Connection;
import net.fly78.miniSQL.util.TableStructBean;

public class Insert extends CommandAdpter {
	public static final String INSERT = "insert into";

	@Override
	public void excuse(String cmd) {
		this.cmdstart = new Date();
		String cmdstr = StringUtils.trim(StringUtils.replace(cmd, INSERT, ""));
		String[] cmdarray = StringUtils.split(cmdstr," ");
		
		try {
			File table = Connection.getInstance().getTable(StringUtils.trim(cmdarray[0]));
			List<TableStructBean> ordertable = new ArrayList<TableStructBean>();
			BufferedReader br = new BufferedReader(new FileReader(table));
			String f = br.readLine();
			String tc =f.split(" ")[0];
			for(int i = 0;i<Integer.parseInt(tc);i++){
				String tableDetil = br.readLine();
				String[] tda = tableDetil.trim().split(" ");
				TableStructBean tsb = new TableStructBean();
				tsb.setName(tda[0].toLowerCase());
				tsb.setTypeName(tda[1].toLowerCase());
				tsb.setType(TypeMap.HS.get(tda[1]));
				tsb.setMaxLength(Integer.parseInt(tda[2]));
				ordertable.add(tsb);
			}
			br.close();
			String value = StringUtils.split(cmdstr,"(")[1].replace(")","").trim();
			BufferedWriter bw = new BufferedWriter(new FileWriter(table, true));
			bw.append("\n"+value);
			bw.close();
			
			this.cmdend = new Date();
			System.out.println( String.format("( 1 record insert , time: %.2f sec)", (float)(this.cmdend.getTime()-this.cmdstart.getTime())/1000)  );
		} catch (Exception e) {
			logger.error(e.toString());
			System.out.println("runtime error please retry");
		}
	}
	
	
}
