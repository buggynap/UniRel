import java.io.IOException;
import java.util.Random;


public class unirel {

	public static void main(String[] args) throws IOException {
		
		//Generate the two relations
		
		relationGenerator relation1 = new relationGenerator(4);
		relation1.generateRelation("table1", 10, 15);
		relation1.generateRelation("table2", 100, 15);
		
		/*relationGenerator relation2 = new relationGenerator("table2", 2, 100);
		relation2.generateRelation(15);*/
		
		System.out.println(relation1.getSizeOfRecord());
		
	}

}
