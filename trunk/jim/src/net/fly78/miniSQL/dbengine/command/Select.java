package net.fly78.miniSQL.dbengine.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import net.fly78.miniSQL.datatype.DataType;
import net.fly78.miniSQL.datatype.TypeMap;
import net.fly78.miniSQL.dbengine.Connection;
import net.fly78.miniSQL.parser.SqlParser;
import net.fly78.miniSQL.parser.SqlParser.ConditionBean;
import net.fly78.miniSQL.util.PrintTools;
import net.fly78.miniSQL.util.TableStructBean;

public class Select extends CommandAdpter {
	public static final String SELECT = "select";
	
	private List<List<DataType>> al ;
	private List<TableStructBean> ordertable;
	public List<TableStructBean> getOrdertable() {
		return ordertable;
	}

//	public void setOrdertable(List<TableStructBean> ordertable) {
//		this.ordertable = ordertable;
//	}

	public List<List<DataType>> getAl() {
		return al;
	}

//	public void setAl(List<List<DataType>> al) {
//		this.al = al;
//	}

	@Override
	public void excuse(String cmd) {
		this.cmdstart = new Date();
		//String subname = StringUtils.trim(StringUtils.replace(cmd, SHOW, ""));
		try {
			
			
			SqlParser sp = new SqlParser(cmd);
			
			
			if((sp.getConditionList()==null || sp.getConditionList().size()<=10) && sp.getGroupCols()==null && sp.getOrderCols()==null &&sp.getTablesName().length==1){
				this.nowhere(sp);
			}else{
				System.out.println("coming soon :)");
			}
			
			
			this.cmdend = new Date();
			System.out.print( String.format("( time: %.2f sec)", (float)(this.cmdend.getTime()-this.cmdstart.getTime())/1000)  );
			System.out.println();
		} catch (Exception e) {
			//e.printStackTrace();
			logger.error(e.toString());
			System.out.println("runtime error please retry");
		} 
	}
	
	/*
	public void nowhere(SqlParser sp) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException{
		String tablename = sp.getTables();
		File table = Connection.getInstance().getTable(tablename);
		BufferedReader br = new BufferedReader(new FileReader(table));
		String f = br.readLine();
		String tc =f.split(" ")[0];

		//获取表结构
		Hashtable<String,String> tableStruct = new Hashtable<String,String>();
		List<Hashtable> ordertable = new ArrayList<Hashtable>();
		for(int i = 0;i<Integer.parseInt(tc);i++){
			String tableDetil = br.readLine();
			String[] tda = tableDetil.trim().split(" ");
			tableStruct.put(tda[0], tda[1]);
			ordertable.add(tableStruct);
		}
		int i = 1;
		String data ="";
		List<DataType[]> al = new ArrayList<DataType[]>();
		while((data=br.readLine())!=null){
			
			
			String dataA[] = data.trim().split(" ");
			
			i++;
		}
	}
	*/
	public void nowhere(SqlParser sp) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException{
		
		
		String tablename = sp.getTables();
		File table = Connection.getInstance().getTable(tablename.trim());
		BufferedReader br = new BufferedReader(new FileReader(table));
		String f = br.readLine();
		String tc =f.split(" ")[0];
		Hashtable<String,Boolean> hsb = new Hashtable<String,Boolean>();
		Hashtable<String,ConditionBean> hsc = sp.getConHS();
		
		for(String str:sp.getColsName()){
			hsb.put(str.trim().toLowerCase(), true);
		}
		//获取表结构
		ordertable = new ArrayList<TableStructBean>();
		for(int i = 0;i<Integer.parseInt(tc);i++){
			String tableDetil = br.readLine();
			String[] tda = tableDetil.trim().split(" ");
			TableStructBean tsb = new TableStructBean();
			tsb.setName(tda[0].toLowerCase());
			tsb.setTypeName(tda[1].toLowerCase());
			tsb.setType(TypeMap.HS.get(tda[1].toUpperCase()));
			tsb.setMaxLength(Integer.parseInt(tda[2]));
		
//	System.out.println(sp.getCols());
//	System.out.println(tda[0].trim().toLowerCase());
//	System.out.println(hsb.get(tda[0].trim().toLowerCase()));
	
			if(sp.getCols().trim().equals("*")||(hsb.get(tda[0].trim().toLowerCase())!=null && hsb.get(tda[0].trim().toLowerCase())) ){
				tsb.setOnSelect(true);
			}else{
				tsb.setOnSelect(false);
			}
			if(hsc.get(tda[0].trim().toLowerCase())!=null){
				tsb.setCb(hsc.get(tda[0].trim().toLowerCase()));
			}
//	System.out.println(tsb.isOnSelect());		
//	System.out.println("****************************");
			ordertable.add(tsb);
		}
		List<Integer> widthList = new ArrayList<Integer>();
		String data ="";
		al = new ArrayList<List<DataType>>();
		int i = 0;
		boolean tag = true;
		while((data=br.readLine())!=null){
			try{
				if(StringUtils.isEmpty(data.trim())||StringUtils.isBlank(data.trim())||StringUtils.isWhitespace(data.trim()))continue;
				//if("".equals(data.trim()))continue;
				String dataA[] = data.trim().split(" ");
				
				List dt = new ArrayList<List<DataType>>();//DataType[hsb.size()];
				for(int j=0;j<dataA.length ;j++){
					//dataA[j] = new DataType(dataA[j]);
					
					 DataType dtttt = (DataType) (ordertable.get(j).getType().getConstructor(String.class).newInstance(new Object[]{dataA[j]}));
					 if(ordertable.get(j).getCb()!=null){
						 tag = this.comp(dtttt, ordertable.get(j).getCb(),tag);
				     }
					 if(ordertable.get(j).isOnSelect()){
						 try{
	//		System.out.println(dtttt.getValue() +"   >>>>>length="+dtttt.getLength());
							if(widthList.get(j)<dtttt.getLength()){
								 widthList.set(j, dtttt.getLength());
							}
						 }catch(Exception e){
							// e.printStackTrace();
							 widthList.add( dtttt.getLength());
						 }
	//	 for(Integer iw:widthList){
	//		System.out.println("===>处理字体"+iw);
	//	}
						 
						 
						 
						 
						 //System.out.println(ordertable.get(j).getCb());
						 dt.add(dtttt);
					 }
				}
				//op = = , fieldName = condi6 , logic = and , value = 6,
				
				
	//			System.out.println();
				if(tag){
					al.add(dt);
					i++;
				}
			}catch(Exception e){
				//e.printStackTrace();
			}
			
		}
		
		PrintTools.printTableTitle(ordertable, widthList);
		PrintTools.printStartNewLine();
		PrintTools.printBorderLine(30);
		PrintTools.printStartNewLine();
		PrintTools.printDate(al, widthList);
		PrintTools.printStartNewLine();
		PrintTools.printOnlyText(String.format("%d rows in set",i));
		
	}
	
	private boolean comp(DataType dt , ConditionBean cb,boolean n){
		boolean tag = false;
		if(cb.getOp().equals(SqlParser.ConditionBean.OP_BIGGERTHEN)){
			 if (dt.compare(cb.getValue())==1)tag = true;else tag = false;
		}else if(cb.getOp().equals(SqlParser.ConditionBean.OP_SMALLERTHEN)){
			 if (dt.compare(cb.getValue())==-1)tag = true;else tag = false;
		}else if(cb.getOp().equals(SqlParser.ConditionBean.OP_EQUAL)){
			 if (dt.compare(cb.getValue())==0)tag = true;else tag = false;
		}else if(cb.getOp().equals(SqlParser.ConditionBean.OP_BIGGEREQUAL)){
			 if (dt.compare(cb.getValue())==1||dt.compare(cb.getValue())==0)tag = true;else tag = false;
		}else if(cb.getOp().equals(SqlParser.ConditionBean.OP_SMALLEREQUAL)){
			 if (dt.compare(cb.getValue())==-1||dt.compare(cb.getValue())==0)tag = true;else tag = false;
		}else if(cb.getOp().equals(SqlParser.ConditionBean.OP_NOTEQUAL)){
			 if (dt.compare(cb.getValue())!=0)tag = true;else tag = false;
		}
		
		
		if(cb.getLogic().equals(SqlParser.ConditionBean.LOGIC_AND)){
			tag = n&&tag;
		}else if(cb.getLogic().equals(SqlParser.ConditionBean.LOGIC_OR)){
			tag = n||tag;
		}
		
		return tag;
	}
	
	//op = = , fieldName = condi6 , logic = and , value = 6,
//	private boolean recuse(List<TableStructBean> tsbl , int i,boolean b){
//		boolean tag = true;
//		try{
//			if(tsbl.size()<=i){
//				if(tsbl.get(i).getCb()!=null){
//					if(tsbl.get(i).getCb().getLogic().equals(SqlParser.ConditionBean.LOGIC_AND)){
//						//return this.recuse(tsbl, ++i, b&&);
//					}else{
//						
//					}
//				}
//			}
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//		
//		return tag;
//	}
}
