package net.fly78.miniSQL.parser.engine;

public class AnalyzeReslut {
	 private SyntaxItem FItem; // �����﷨��
     private String FBlock;// ���ֿ�
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
