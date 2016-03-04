import java.io.IOException;

public class unirel {
	
	public static int sizeOfRecord = 0;

	public static void main(String[] args) throws IOException {
		
		//	Check for the command line arguments
		
		// 	M - Block size; should always less than 1 MB
		int M = 1; // MB
		String typeOfIndex = null;
		
		if(args.length != 2) {
			System.out.println("unirel <BLOCK_SIZE> <TYPE_OF_INDEX>");
			System.exit(0);
		}
		
		M = Integer.parseInt(args[0]);
		typeOfIndex = args[1];
		
		long startTime = System.currentTimeMillis();
		
		//Generate the two relations
		relationGenerator relation1 = new relationGenerator(4);
		System.out.print("Generating relation1...");
		relation1.generateRelation("table1", 5, 15);
		System.out.println("[OK]");
		System.out.print("Generating relation2...");
		relation1.generateRelation("table2", 500 , 15);
		System.out.println("[OK]");
		sizeOfRecord = relation1.getSizeOfRecord();
		
		System.out.println("Starting union...");
		//	Call the union procedure		
		union("table1", "table2", relation1.getNumberOfAttributes(), M, typeOfIndex);
		
		long endTime = System.currentTimeMillis();
		System.out.println("\nExecution Time : " + (endTime - startTime) + "ms");
	}
	
	private static void union(String relation1, String relation2, int noOfAttributes, int blockSize, String typeOfIndex) throws IOException {
		
		System.out.print("Method detected...");
		
		if(typeOfIndex.compareToIgnoreCase("HASH") == 0) {
			System.out.print("[HASH]");
			hashedUnion hu = new hashedUnion();
			hu.union(relation1, relation2, noOfAttributes, blockSize, unirel.sizeOfRecord, 36);
			
		} else {
			
			System.out.print("[B+ TREE]");
			BTreeUnion bu = new BTreeUnion();
			//bu.union(relation1, relation2, noOfAttributes, blockSize, unirel.sizeOfRecord);
			
		}
	}
}