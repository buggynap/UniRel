import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;


public class relationGenerator {
	
	private int noOfColumns;
	private int sizeOfRelation;
	private String relationName;
	private String[] randomMetadataArray = new String[]{"char(10)", "int", "date"};
	private HashMap <String, String> colMetaDataPair = new HashMap <String, String>();
	private int sizeOfRecord = 0;
	
	public relationGenerator(int noOfColumns) throws IOException {
		
		this.noOfColumns = noOfColumns;
		
		Random ran = new Random();
		
		//	Generate the metadata file in case if we need it
		// 	Get the list of the metadata for the current relation
		
		BufferedWriter metadataWriter = new BufferedWriter(new FileWriter(new File("metadata.txt")));
		
		for(int i = 0; i < noOfColumns; i++) {
			
			int x = ran.nextInt(3);
			String colDataType = randomMetadataArray[x];
			
			//Get the size of the single record
			switch(colDataType) {
				
				case "char(10)":
					sizeOfRecord += 10;
					break;
				case "date":
					sizeOfRecord += 10;
					break;
				case "int":
					sizeOfRecord += 6;
					break;
			}
			//for comma
			sizeOfRecord += 1;
			colMetaDataPair.put("col" + String.valueOf(i + 1) , colDataType);
			metadataWriter.write("col" + String.valueOf(i + 1) + "," + colDataType);
			if(i + 1 != noOfColumns)
				metadataWriter.write("\n");
		}
		//for new line
		sizeOfRecord += 1;
		
		metadataWriter.close();
	}
	
	public void generateRelation(String relationName, int sizeOfRelation, int duplicationCount) throws IOException {
		
		this.sizeOfRelation = sizeOfRelation * 1024 * 1024;
		this.relationName = relationName;
		
		//	Generate the relation as per the random metadata generated
		
		FileWriter relationWriter = new FileWriter(new File(this.relationName));
		int currentRelationSize = 0, recordCounter = 0;
		String characters = null;
		List <String> records = new ArrayList <String>();
		ArrayList<Integer> list = new ArrayList<Integer>();
	    for (int i = 0; i < 100; i++) {
	    	list.add(i);
	    }
		
		while(currentRelationSize < this.sizeOfRelation) {
			
			StringBuffer record = new StringBuffer(sizeOfRecord);
			
			for(int i = 0; i < noOfColumns; i++) {
				
				switch (colMetaDataPair.get("col" + String.valueOf(i + 1))) {
				
					case "char(10)" :
						characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
						int charactersLength = characters.length();
						int j = 0;
						while (j < 10) {
							double index = Math.random() * (double)charactersLength;
						    record.append(characters.charAt((int)index));
						    ++j;
						 }
						break;
						
					case "int" : 
						int min = 1;
						int max = 999999;
						Random random = new Random();
						Integer randomNumber = random.nextInt(max - min) + min;
						record.append(randomNumber);
						break;
					
					case "date": 
						long beginTime = Timestamp.valueOf("1945-01-01 00:00:00").getTime();
						long endTime = Timestamp.valueOf("2015-12-31 00:58:00").getTime();
						long diff = endTime - beginTime + 1;
						long newDate = beginTime + (long)(Math.random() * (double)diff);
						SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
						newDate = beginTime + (long)(Math.random() * (double)diff);
						Date randomDate = new Date(newDate);
						record.append(dateFormat.format(randomDate));
						break;
		        }
				//to append the comma after every column and also we dont want the comma after last columns
				if(i + 1 != noOfColumns)
					record.append(',');
			}
			// count if the record are equal to 100 
			if(recordCounter++ == 100) {
				// Get the random records from the generated records
				Collections.shuffle(list);
				for(int k = 0; k < duplicationCount; k++) {
					records.add(records.get(list.get(k)));
				}
				//Write to the file
				for(int k = 0; k < 100 + duplicationCount; k++) {
					relationWriter.write(records.get(k) + "\n");
					currentRelationSize += sizeOfRecord;
				}
				records.clear();
				recordCounter = 0;
			} else {
				records.add(record.toString());
			}
		}
		relationWriter.close();
	}
	
	public int getSizeOfRecord() {
		
		return this.sizeOfRecord;		
	}
}

