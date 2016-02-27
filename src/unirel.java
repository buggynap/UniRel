import java.io.IOException;
import java.util.Random;


public class unirel {

	public static void main(String[] args) throws IOException {
		
		//Generate the two relations
		
		relationGenerator relation1 = new relationGenerator("table1", 4, 10);
		relation1.generateRelation(15);
		
		relationGenerator relation2 = new relationGenerator("table1", 4, 10);
		relation2.generateRelation(15);
		
	}

}
