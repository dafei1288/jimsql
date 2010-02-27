package net.fly78.miniSQL.util;

import net.fly78.miniSQL.parser.SqlParser.ConditionBean;

public class TableStructBean {
	private String name;
	private String typeName;
	private Class type;
	private int maxLength;
	private boolean onSelect = false;
	private ConditionBean cb;
	
	public ConditionBean getCb() {
		return cb;
	}
	public void setCb(ConditionBean cb) {
		this.cb = cb;
	}
	public boolean isOnSelect() {
		return onSelect;
	}
	public void setOnSelect(boolean onSelect) {
		this.onSelect = onSelect;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getTypeName() {
		return typeName;
	}
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	public Class getType() {
		return type;
	}
	public void setType(Class type) {
		this.type = type;
	}
	public int getMaxLength() {
		return maxLength;
	}
	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}
	
	
	
	
}
