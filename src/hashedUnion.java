import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class hashedUnion {
	
	private static long currentFilePointerPosition = 0;
	private static int noOfRecordsInM = 0;
	public static HashMap<Character, HashSet <String> > buckets = new HashMap<Character, HashSet <String>>();
	List <String> bucketFileNames = new ArrayList<String>();
	
	public void union(String relation1, String relation2, int noOfAttributes, int blockSize, int sizeOfRecord, int noOfBuffers) throws IOException {
		
		//process first relation
		noOfRecordsInM = (blockSize * 1000 * 1000) / sizeOfRecord;
		RandomAccessFile rafPointer = open(relation1);
		processrelation(rafPointer, noOfBuffers, blockSize);
		close(rafPointer);
		
		//	Reset the seek pointer
		currentFilePointerPosition = 0;
		rafPointer = open(relation2);
		processrelation(rafPointer, noOfBuffers, blockSize);
		close(rafPointer);
		
		//	loop through the list of the bucket files and call merge
		String outputFile = "output.txt";
		int totalBucketFileCount = bucketFileNames.size();
		for(int i = 0; i < totalBucketFileCount; i++)
			finalMerge(outputFile, bucketFileNames.get(i));
	}

	private void finalMerge(String outputFile, String bucketFile) throws IOException {
		
		BufferedReader bfrBucketFile = new BufferedReader(new FileReader(new File(bucketFile)));
		FileWriter fwrOutputFile = new FileWriter(new File(outputFile), true);
		
		HashSet <String> bucketsForRecords = new HashSet<String>();
		String line = null;
		
		while((line = bfrBucketFile.readLine()) != null) {
			if(!bucketsForRecords.contains(line))
				bucketsForRecords.add(line);
		}
		
		Iterator iterator = bucketsForRecords.iterator();
		
		while (iterator.hasNext()){
			fwrOutputFile.write(iterator.next() + "\n");  
		}
		fwrOutputFile.close();
		
		//	Delete the file we don't want it
		if(!new File(bucketFile).delete()) {
			System.err.println("Error in deletion...");
		}
		
	}

	private void processrelation(RandomAccessFile rafPointer, int noOfBuffers, int blockSize) throws IOException {
		
		//	loop through the relation and get the data of size "M" and divide it in the M-1 blocks as buckets, we will keep M as 36 
		//	let's start
		
		List <String> recordsInSingleBlock = new ArrayList<String>();
		
		while((recordsInSingleBlock = getNext(rafPointer, blockSize)) != null) {
			
			int totalRecordsInM = recordsInSingleBlock.size();
			
			if(totalRecordsInM == 0)
				break;
			
			//Distribute the records in the M-1 buckets i.e. on 36 buckets
			
			for(int i = 0; i < totalRecordsInM; i++) {				
				char currentRecordStartCharacter = recordsInSingleBlock.get(i).toLowerCase().charAt(0);
				if(buckets.get(currentRecordStartCharacter) == null) {					
					buckets.put(currentRecordStartCharacter, new HashSet<String>());
				} else {
					buckets.get(currentRecordStartCharacter).add(recordsInSingleBlock.get(i));					
					int totalRecordsInBucket = buckets.get(currentRecordStartCharacter).size();					
					//Write it into the file
					if(totalRecordsInBucket >= noOfRecordsInM) {
						Iterator<String> iterator = buckets.get(currentRecordStartCharacter).iterator();
						String bucketFileName = currentRecordStartCharacter + "_bucket";
						
						//	Add entry of the filename in the list for final processing
						if(!bucketFileNames.contains(bucketFileName)) {
							bucketFileNames.add(bucketFileName);
						}
						BufferedWriter bfrBucket = new BufferedWriter(new FileWriter(new File(bucketFileName), true));
						
						while (iterator.hasNext()){
						   bfrBucket.write(iterator.next() + "\n");
						}						
						bfrBucket.close();
						buckets.get(currentRecordStartCharacter).clear();
					}
				}
			}
		}
		//check for the remaining records	
		Iterator<Map.Entry<Character, HashSet<String>>> itr1 = buckets.entrySet().iterator();
		while (itr1.hasNext()) {
		    Map.Entry<Character, HashSet<String>> entry = itr1.next();
			String bucketFileName = entry.getKey() + "_bucket";
			
			if(!bucketFileNames.contains(bucketFileName)) {
				bucketFileNames.add(bucketFileName);
			}
		    BufferedWriter bfrBucket = new BufferedWriter(new FileWriter(new File(bucketFileName), true));
		    Iterator<String> itr2 = entry.getValue().iterator();
		    while (itr2.hasNext()) {
		    	bfrBucket.write(itr2.next() + "\n");
		    }
		    bfrBucket.close();
		    entry.getValue().clear();
		}
		recordsInSingleBlock.clear();
	}


	private List <String> getNext(RandomAccessFile rafPointer, int blockSize) throws IOException {
		
		long startTime = System.currentTimeMillis();
		
		List <String> loadRecordsOfSizeM = new ArrayList<String>();
		String line = null;
		int currentSize = 0;
		
		rafPointer.seek(currentFilePointerPosition);
		
		while((line = rafPointer.readLine()) != null) {
			loadRecordsOfSizeM.add(line);
			currentFilePointerPosition += line.length() + 1;
			currentSize += line.length() + 1;
			if(currentSize >= blockSize * 1000 * 1000)
				break;
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Time taken : " + (endTime - startTime));
		return loadRecordsOfSizeM;
	}

	private RandomAccessFile open(String relation1) throws FileNotFoundException {
		
		RandomAccessFile fileHandle = new RandomAccessFile(new File(relation1), "r");
		return fileHandle;
	}
	
	private void close(RandomAccessFile filePointer) throws IOException {
		
		filePointer.close();
	}
}
