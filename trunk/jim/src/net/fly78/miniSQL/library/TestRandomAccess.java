package net.fly78.miniSQL.library;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class TestRandomAccess {

	public static void main(String[] args) throws IOException {
		File f = new File("d:\\tttttt");
		RandomAccessFile raf = new RandomAccessFile(f,"rw");
//		raf.writeUTF("i love u");
//		for(int i=0;i<65536;i++){
//			raf.writeUTF("i love u"+i);
//		}
//		raf.close();
		
		String res = "";
		for(int i=0;i<1024;i++){
			System.out.println(raf.readLine());
		}
		raf.writeChars("insert it in here.......");
		raf.close();
		
	}

}
