package net.fly78.miniSQL.library;

import org.fest.reflect.method.MethodParameterTypes;

public class HelloFest implements IHF{
	public String say(String res,int a){
		System.out.println("Hello wodld !"+ this.hashCode());
		
		System.out.println("input "+res +" , "+a);
		return res+" / "+a;
	}
	
	public void Hello(){
		System.out.println("==========>  这个   <===========");
	}
	
	public static void main(String[] args){
		HelloFest hf = org.fest.reflect.core.Reflection.constructor().in(HelloFest.class).newInstance(null);
		hf.say("dafei1288", 999);
		
	}
	
	public String toString(){
		return "==========>  这个   <===========";
	}

	@Override
	public String toH() {
		return "==========>  这个   <===========";
	}
}
