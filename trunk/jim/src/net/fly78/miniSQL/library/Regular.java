package net.fly78.miniSQL.library;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Regular {

	public static void main(String[] args) {
		String r = "where c=1 and c1<2 or  c3>4";
		String r1=" aaa < 2 ";
		
		String regular = "(and|or)";
		
		Pattern p = Pattern.compile(regular);    
        Matcher m = p.matcher(r);    
		
        while(m.find()){
        	int i = m.groupCount();
        	//System.out.println(" ==> "+i+"   /  "+m.group());
        	for(int j=0;j<i;j++){
        		System.out.println(r.substring(0,m.start()));
        		System.out.println(r.substring(m.start(),m.end()));
        		System.out.println(r.substring(m.end()));
        		System.out.println(" ==> "+i+"   /  "+m.group(j));
        	}
        }
		
		
	}

}
