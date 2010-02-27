package net.fly78.miniSQL.util;

public class QuickSort {

    public static void quickSort(int[] array) {
        quickSort(array, 0, array.length - 1);
    }

    private static void quickSort(int[] array, int low, int high) {
        if (low < high) {
            int p = partition(array, low, high);
            quickSort(array, low, p - 1);
            quickSort(array, p + 1, high);
        }

    }

    private static int partition(int[] array, int low, int high) {
        int s = array[high];
        int i = low - 1;
        for (int j = low; j < high; j++) {
            if (array[j] < s) {
                i++;
                swap(array, i, j);
            }
        }
        swap(array, ++i, high);
        return i;
    }
    
    private static void swap(int[] array, int i, int j) {
        int temp;
        temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
    
    
    
    
    public static void quickSortGt(int[] array){
    	quickSort(array, 0, array.length - 1);
    }
    
    public static void quickSortLt(int[] array){
    	quickSort(array, 0, array.length - 1);
    	java.util.Arrays.sort(array);
    }
    
    
    public static void main(String[] args) {
        int [] array = {2,5,3,7,4};
        quickSort(array);
        for(int i = 0;i<array.length;i++){
            System.out.println(array[i]);
        }
    }

}