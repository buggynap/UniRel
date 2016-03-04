import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class HashedUnion {
	
	private int sizeOfRecord = 0;	
	private static HashSet<String> hashedRecords = new HashSet<String>();
	private static long filePointerPosition = 0;

	public void union(String relation1, String relation2, int noOfAttributes, int blockSize, int sizeOfRecord) throws IOException {
		
		/* 	Will consider the hash set for checking the duplicate values and check the records equal
		 * 	block-size, when the records will exceeds the block-size and then write the records to the file
		 * 	and again check for the records and continue the process
		 */
		
		/*	Assumption is : relation1 will always fit in the memory and second will not thats why we will use 
		 * 	block size for the second relation to union
		 */
		
		this.sizeOfRecord = sizeOfRecord;
		BufferedReader bfinputrelation1 = new BufferedReader(new FileReader(new File(relation1)));
		//BufferedReader bfinputrelation2 = new BufferedReader(new FileReader(new File(relation2)));
		RandomAccessFile rAFrelation2 = new RandomAccessFile(new File(relation2), "r");
		FileWriter foutputRelation = new FileWriter(new File("output"));
		List <String> records = new ArrayList<String>();
		
		String record1 = null, record2 = null;
		int totalRecordLength = 0;
		boolean finalWrite = false;
		
		/*while((record1 = bfinputrelation1.readLine()) != null || (record2 = bfinputrelation2.readLine()) != null) {
			
			//	set flag 
			finalWrite = false;
			
			if(record1 != null && !hashedRecords.contains(record1)) {
				hashedRecords.add(record1);
				records.add(record1);
				totalRecordLength += sizeOfRecord;
			}
			if(record2 != null && !hashedRecords.contains(record2)) {
				hashedRecords.add(record2);
				records.add(record2);
				totalRecordLength += sizeOfRecord;
			}
			if(totalRecordLength >= blockSize * 1024 * 1024){
				
				int recordCount = records.size();
				for(int i = 0; i < recordCount; i++) {
					foutputRelation.write(records.get(i) + "\n");
				}
				records.clear();		
				finalWrite = true;
				totalRecordLength = 0;
			}
		}
		
		if(finalWrite == false) {
			
			int recordCount = records.size();
			for(int i = 0; i < recordCount; i++) {
				foutputRelation.write(records.get(i) +"\n");
			}
			records.clear();
		}*/
		
		//	Trying Random access file iterator
		
		// 	Read the records of the first relation and write it into the output file
		while((record1 = bfinputrelation1.readLine()) != null)
		{
			finalWrite = false;
			
			if(record1 != null && !hashedRecords.contains(record1)) {
				
				hashedRecords.add(record1);
				records.add(record1);
				totalRecordLength += sizeOfRecord;
			}
			
			if(totalRecordLength >= blockSize * 1024 * 1024){
				
				int recordCount = records.size();
				for(int i = 0; i < recordCount; i++) {
					foutputRelation.write(records.get(i) + "\n");
				}
				records.clear();		
				finalWrite = true;
				totalRecordLength = 0;
			}
		}
		
		while((records = getNextChunk(rAFrelation2, blockSize)) != null) {
			
			if(records.size() == 0){
				break;
			}
			
			int currentTotalRecords = records.size();
			for(int j = 0; j < currentTotalRecords; j++) {
				
				if(!hashedRecords.contains(records.get(j))) {
					hashedRecords.add(records.get(j));
					foutputRelation.write(records.get(j) + '\n');
				}
			}
			records.clear();
		}		
		bfinputrelation1.close();
		rAFrelation2.close();
		//bfinputrelation2.close();
		foutputRelation.close();
	}
	
	public List<String> getNextChunk(RandomAccessFile relation2, int bufferSize) throws IOException
	{
		System.out.println("I am getting the list...");
		
		int currentSize = 0;
		String record = null;
		boolean finalCall = false;
		List<String> blockSizedRecords = new ArrayList<String>();
		
		//seek to the position 
		relation2.seek(filePointerPosition);
		
		//	Read the records till "M" block size
		
		while((record = relation2.readLine())!= null) {
			finalCall = false;
			blockSizedRecords.add(record);
			currentSize += record.length() + 1;
			filePointerPosition += record.length() + 1;
			if(currentSize >= bufferSize * 1024 * 1024) {
				break;
			}
		}
		System.out.println("I am got the list...  " + filePointerPosition);
		if(blockSizedRecords.isEmpty())
			System.out.println("I am empty");
		return blockSizedRecords;
	}
}