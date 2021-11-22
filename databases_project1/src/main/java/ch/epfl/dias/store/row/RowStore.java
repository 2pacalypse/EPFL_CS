package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	// TODO: Add required structures

	List<DBTuple> tuples;
	
	DataType[] schema;
	String filename;
	String delimiter;
	
	public RowStore(DataType[] schema, String filename, String delimiter) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuples = new ArrayList<DBTuple>();
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement
		
		String workingDir = System.getProperty("user.dir");

        Path filePath = Paths.get(workingDir + File.separator + filename);
		
		BufferedReader br = new BufferedReader(new FileReader(filePath.toFile()));
		String line;
		
		while( (line = br.readLine()) != null) {
			Scanner scanner = new Scanner(line);
			scanner.useDelimiter(delimiter);
			
			Object[] fields = new Object[schema.length];
			
			for(int i = 0; i < schema.length; i++) {
				DataType dt = schema[i];
				
				switch (dt){
				case BOOLEAN:	
					fields[i] = scanner.nextBoolean();
					break;
				case DOUBLE:
					fields[i] = scanner.nextDouble();
					break;
				case INT:
					fields[i] = scanner.nextInt();
					break;
				case STRING:
					fields[i] = scanner.next();
					break;
				}

			}
			scanner.close();
			
			DBTuple tuple = new DBTuple(fields,schema);
			tuples.add(tuple);
			
			
		}
		br.close();
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		return tuples.get(rownumber);
	}
}
