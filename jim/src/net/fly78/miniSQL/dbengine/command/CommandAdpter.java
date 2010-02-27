package net.fly78.miniSQL.dbengine.command;

import java.util.Date;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandAdpter implements Command {
	protected String cmd; 
	protected Date cmdstart;
	protected Date cmdend;
	protected Logger logger =  LoggerFactory.getLogger(CommandAdpter.class);
	@Override
	public void excuse(String cmd) {
		this.cmd = cmd;
		System.out.println(cmd);
	}

}
