import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

public class ArrowHelloWorld {
    public static void main(String[] args) {
        // Create an allocator to manage memory (automatically closed via try-with-resources)
        try (RootAllocator allocator = new RootAllocator();
             IntVector intVector = new IntVector("numbers", allocator)) {

            // Initialize vector with 5 values
            intVector.allocateNew(5);
            intVector.set(0, 1);
            intVector.set(1, 2);
            intVector.set(2, 3);
            intVector.set(3, 4);
            intVector.set(4, 5);
            intVector.setValueCount(5);

            // Print the vector contents
            System.out.println("Arrow Vector Contents:");
            for (int i = 0; i < intVector.getValueCount(); i++) {
                System.out.println(intVector.get(i));
            }
        }
    }
}