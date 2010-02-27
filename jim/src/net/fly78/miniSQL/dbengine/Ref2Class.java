package net.fly78.miniSQL.dbengine;

import net.fly78.dafei1288.util.properties.EnvironmentConfig;
import net.fly78.miniSQL.dbengine.command.Command;

public class Ref2Class {
	public static Command getCommand(String comd) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		return (Command) Class.forName(EnvironmentConfig.getInstance("classmap").getPropertyValue(comd)).newInstance();
	}
}
