package net.fly78.miniSQL.util;

import java.util.List;

import net.fly78.miniSQL.datatype.DATE;
import net.fly78.miniSQL.datatype.DataType;
import net.fly78.miniSQL.datatype.FLOAT;
import net.fly78.miniSQL.datatype.INT;
import net.fly78.miniSQL.datatype.STR;


public class PrintTools {
	public static void printContext(String name , int length){
		int printnum = Math.max(name.getBytes().length, length);//name.length()<(length-4)?length:name.length()+4;
		
		int cc = 0;
		
		int tt= 0;
		printStartNewLine();
		tt+=printComplexLine(printnum);
		
		
		printStartNewLine();
		printTextLine(name,printnum);
		
		
		printStartNewLine();
		printComplexLine(printnum);

		printStartNewLine();
		
		
	}
	
	public static void printOnlyText(String text){
		System.out.print(text);
	}
	
	public static void printTableTitle(List<TableStructBean> ts,List<Integer> widthList){
		for(int i=0;i<ts.size();i++){
			try{
				widthList.get(i);
			}catch(Exception e){
				widthList.add(0);
			}
			TableStructBean tsb = ts.get(i);
			if(tsb.isOnSelect()){
				printTextLineWithoutRight(tsb.getName(),widthList.get(i));
			}
		}
	}
	
	public static void printDate(List<List<DataType>> al,List<Integer> j){
		for(List<DataType> ldp : al){
			for(int i=0;i<ldp.size();i++){
				printTextLineWithoutRight(ldp.get(i).getStringValue(),j.get(i));
			}
			printStartNewLine();
		}
	}
	
	public static void printData(DataType[][] data,int[] i){
		for(DataType[] dd : data){
			printRowDate(dd,i);
			printStartNewLine();
		}
	}
	public static void printRowDate(DataType[] data,int[] i){
		for(int j=0;j<data.length;j++){
			//DataType d : data
			printTextLineWithoutRight(data[j].getStringValue(),i[j]);
		}
	}
	
	public static int printComplexLine(int i){
		int tt=0;
		tt+=printConnerChar();
		tt+=printBorderLine(i-2);
		tt+=printConnerChar();
		return tt;
	}
	
	public static int printTextLineWithoutRight(String text,int i){
		int tt=0;
		tt+=printBorderVerticalChar();
		tt+=printOneWhite();
		tt+=printText(text);
		printWhiteLine(i-tt-1);
		//tt+=printBorderVerticalChar();
		return tt;
	}
	
	
	public static int printTextLine(String text,int i){
		int tt=0;
		tt+=printBorderVerticalChar();
		tt+=printOneWhite();
		tt+=printText(text);
		printWhiteLine(i-tt-1);
		tt+=printBorderVerticalChar();
		return tt;
	}
	
	
	public static int printOneWhite(){
		System.out.print((char)0);
		return 1;
	}
	public static int printWhiteLine(int i ){
		i = Math.max(i, 0);
		for(int j=0;j++<i;printOneWhite());
		return i;
	}
	public static int printText(String text){
		System.out.print(text);
		return text.getBytes().length;
	}
	public static int printConnerChar(){
		System.out.print("+");
		return 1;
	}
	public static int printBorderHorizontalChar(){
		System.out.print("-");
		return 1;
	}
	public static int printBorderVerticalChar(){
		System.out.print("|");
		return 1;
	}
	public static int printBorderLine(int i){
		i = Math.max(i, 0);
		for(int j=0;j++<i;printBorderHorizontalChar());
		return i;
	}
	public static void printStartNewLine(){
		System.out.println();
	}
	
	
	public static void main(String[] args){
		PrintTools.printContext("测", 20);
		PrintTools.printContext("测试", 20);
		PrintTools.printContext("abc", 20);
		PrintTools.printContext("ac", 20);
		
		PrintTools.printContext("很长很长很长很长很长很长很长很长很长很长很长很长", 20);
		
		DataType[][] dt = new DataType[10][5];
		for(int i=0;i<10;i++){
			dt[i][0] = new STR("小华"+i);
			dt[i][1] = new FLOAT((float)Math.random());
			dt[i][2] = new INT(i);
			dt[i][3] = new STR("阿沙ashdsdha"+i*i);
			dt[i][4] = new DATE();
		}
		
		//printData(dt);
		
	}
}
