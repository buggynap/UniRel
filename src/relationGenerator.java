import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;


public class relationGenerator {
	
	private int noOfColumns;
	private int sizeOfRelation;
	private String relationName;
	private String[] randomMetadataArray = new String[]{"char(10)", "int", "date"};
	private HashMap <String, String> colMetaDataPair = new HashMap <String, String>();
	private int sizeOfRecord = 0;
	
	public relationGenerator(String relationName, int noOfColumns, int sizeOfRelation) {
		this.relationName = relationName;
		this.noOfColumns = noOfColumns;
		this.sizeOfRelation = sizeOfRelation * 1024 * 1024;
	}
	
	public void generateRelation(int duplicationCount) throws IOException {
		
		Random ran = new Random();
		
		//	Generate the metadata file in case if we need it
		// 	Get the list of the metadata for the current relation
		
		StringBuilder metadataFileName = new StringBuilder();
		metadataFileName.append(this.relationName);
		metadataFileName.append("_metadata.txt");
		
		BufferedWriter metadataWriter = new BufferedWriter(new FileWriter(new File(metadataFileName.toString())));
		
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
			colMetaDataPair.put("col" + String.valueOf(i + 1) , colDataType);
			metadataWriter.write("col" + String.valueOf(i + 1) + "," + colDataType);
			if(i + 1 != noOfColumns)
				metadataWriter.write("\n");
			
			sizeOfRecord += 1;
		}
		
		metadataWriter.close();
		
		//	Generate the relation as per the random metadata generated
		
		FileWriter relationWriter = new FileWriter(new File(this.relationName));
		int currentRelationSize = 0, recordCounter = 0;
		String characters = null;
		
		while(currentRelationSize < sizeOfRelation) {
			
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
			//Write the record to the file
			relationWriter.write(record.toString() + "\n");
			currentRelationSize += sizeOfRecord;
		}
		relationWriter.close();
	}
	
	public int getSizeOfRecord() {
		
		return this.sizeOfRecord;		
	}
}

