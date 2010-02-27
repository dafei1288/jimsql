package net.fly78.miniSQL.parser.engine;

public class AnalyzeReslut {
	 private SyntaxItem FItem; // 所属语法项
     private String FBlock;// 文字块
	public SyntaxItem getFItem() {
		return FItem;
	}
	public String getFBlock() {
		return FBlock;
	}
	public AnalyzeReslut(SyntaxItem item, String block) {
		super();
		FItem = item;
		FBlock = block;
	}
     
     
     
}
