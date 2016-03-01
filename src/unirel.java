import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Random;

public class unirel {
	
	public static int sizeOfRecord = 0;

	public static void main(String[] args) throws IOException {
		
		long startTime = System.currentTimeMillis();
		
		//Generate the two relations
		relationGenerator relation1 = new relationGenerator(4);
		relation1.generateRelation("table1", 5, 15);
		relation1.generateRelation("table2", 10, 15);
		
		sizeOfRecord = relation1.getSizeOfRecord();
		
		//Call the union procedure
		// M - Block size; should always less than 1 MB
		int M = 1; // MB
		union("table1", "table2", relation1.getNumberOfAttributes(), M, "HASH");
		
		long endTime = System.currentTimeMillis();
		System.out.println("Execution Time : " + (endTime - startTime) + "ms");
	}
	
	private static void union(String relation1, String relation2, int noOfAttributes, int blockSize, String typeOfIndex) throws FileNotFoundException {
		
		//get the file handles to the the tablenames
		
		BufferedReader rel1bfr= new BufferedReader(new FileReader(new File(relation1)));
		FileReader rel2bfr= new FileReader(new File(relation2));
		
		if(typeOfIndex == "HASH") {
			
			System.out.println("hash");
			
		} else {
			
			
			
		}
	}
}