package net.fly78.miniSQL.library;
/*http://forums.sun.com/thread.jspa?threadID=5194870
 * Here's a hint to solve your problem in a generic way:

A = [BIG, SMALL, HUGE, TINY]
B = [brown, yellow]
C = [Cat, Dog, Parrot]


 A    B    C

 0    0    0  ==  BIG-brown-Cat
 0    0    1  ==  BIG-brown-Dog
 0    0    2
 0    1    0
 0    1    1
 0    1    2
 1    0    0
 1    0    1
 1    0    2
 1    1    0
 1    1    1
 1    1    2
 2    0    0            ...
 2    0    1
 2    0    2
 2    1    0
 2    1    1
 2    1    2
 3    0    0
 3    0    1
 3    0    2
 3    1    0
 3    1    1
 3    1    2  ==  TINY-yellow-Parrot

column C: repetition                     = 1 = 0, 1, 2, 0, 1, 2, ... 
column B: repetition = C.length          = 3 = 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, ...
column A: repetition = B.length*C.length = 6 = 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, ... (to index 3)
   
 * 
 * */
public class FooCP {
	 private static void bar(String[][] arrays) {
	        int idx = 1;
	        for(int i = 0; i < arrays.length; idx *= arrays[i].length, i++);
	        while(idx-- > 0) {
	            int j = 1;
	            for(String[] a : arrays) {
	                System.out.print(a[(idx/j)%a.length]+" ");
	                j *= a.length;
	            }
	            System.out.println();
	        }
	    }
	    
	    public static void main(String[] args) {
	        bar(new String[][]{
	            {"BIG", "SMALL", "HUGE", "TINY"}, 
	            {"brown", "yellow","black"}, 
	            {"Cat", "Dog", "Parrot"}
	        });
	    }

}
