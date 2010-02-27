package net.fly78.miniSQL.util;

public class Compare {


	public static int[] matrixCompare(int[][] matrix){
		int[] result = new int[matrix[0].length];
		
		int rows = matrix[0].length;
		int colums = matrix.length;
		
		int[][] mr = new int[rows][colums];//[result.length][matrix.length];
		
		for(int i=0;i<mr.length;i++){
			result[i] = 0;
			for(int j=0;j<mr[i].length;j++){
				mr[i][j]=matrix[j][i];
			}
		}
		
		for(int i=0;i<mr.length;i++){
			QuickSort.quickSort(mr[i]);
			result[i]=mr[i][mr[i].length-1];
		}
		
		return result;
	}
	
	
	
	
	public static void main(String[] args) {
		int[][] i = {
				{1,2,4,8,9,43},
				{4,6,1,55,22,11},
				{55,1,5,33,66,22},
				{11,1,5,34,6,7},
		};
		int ia[] = Compare.matrixCompare(i);
		
		for(int a : ia){
			System.out.println(a);
		}
		
	}

}
