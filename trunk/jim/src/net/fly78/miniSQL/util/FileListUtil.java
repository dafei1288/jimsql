package net.fly78.miniSQL.util;

import java.io.File;

public class FileListUtil {


	public static void listFile(String path,String str){
		
		File f = new File(path);
		File[] files = f.listFiles();
		for(File file : files){
			if(f.getName().equals(".")||f.getName().equals("..")){
				return ;
			}
			if(file.isDirectory()){
				System.out.println(str+file.getName()+"/");
				listFile(file.getPath()+"/",str.replaceAll("([+]|[-])"," ")+"+---");
			}else{
				System.out.println(str+file.getName());
			}
		}
		
		
		
		
		
	}
	
	
	public static void main(String[] args) {
		listFile("H:\\miniSQL","+");
	}

}
