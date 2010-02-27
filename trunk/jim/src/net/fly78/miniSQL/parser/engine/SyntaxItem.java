package net.fly78.miniSQL.parser.engine;

import java.util.ArrayList;
import java.util.List;

public class SyntaxItem {
	private String name;// �﷨����
	private String formPattern;// ������ʽ
	private int index;// ���
	public String getName() {
		return name;
	}
	public String getFormPattern() {
		return formPattern;
	}
	public int getIndex() {
		return index;
	}
	public SyntaxItem(String formPattern,String name,  int index) {
		super();
		this.name = name;
		this.formPattern = formPattern;
		this.index = index;
	}
	
	private final static List<SyntaxItem> syntaxItems ;
	static{
		syntaxItems = new ArrayList<SyntaxItem>();
		syntaxItems.add(RegularLibrary.WHITEBLANK);
		syntaxItems.add(RegularLibrary.NOTE);
		syntaxItems.add(RegularLibrary.MULITNOTE);
		syntaxItems.add(RegularLibrary.KEYWORD);
		syntaxItems.add(RegularLibrary.SYMBOL);
		syntaxItems.add(RegularLibrary.INT);
		syntaxItems.add(RegularLibrary.FLOAT);
		syntaxItems.add(RegularLibrary.STR);
		//syntaxItems.add(RegularLibrary.IDENTIFIE);
	}
	public static List<SyntaxItem> getSyntaxItems() {
		return syntaxItems;
	}
	
	
}
