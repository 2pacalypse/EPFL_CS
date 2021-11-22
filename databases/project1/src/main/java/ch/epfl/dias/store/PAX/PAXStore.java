package ch.epfl.dias.store.PAX;

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
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	// TODO: Add required structures
	
	public DataType[] schema;
	public String filename;
	public String delimiter;
	public int tuplesPerPage;
	
	public List<DBPAXpage> paxpages;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
		this.paxpages = new ArrayList<DBPAXpage>();
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement
		String workingDir = System.getProperty("user.dir");
        Path filePath = Paths.get(workingDir + File.separator + filename);
		BufferedReader br = new BufferedReader(new FileReader(filePath.toFile()));
		
		String line;
		int nTuples = 0;
		DBPAXpage currentPage = new DBPAXpage(schema);
		while((line = br.readLine()) != null) {

			
			Scanner scanner = new Scanner(line);
			scanner.useDelimiter(delimiter);
			
			for(int i = 0; i < schema.length; i++) {
				DataType dt = schema[i];
				
				switch (dt){
				case BOOLEAN:	
					currentPage.minipages.get(i).vals.add(scanner.nextBoolean());
					break;
				case DOUBLE:
					currentPage.minipages.get(i).vals.add(scanner.nextDouble());
					break;
				case INT:
					currentPage.minipages.get(i).vals.add(scanner.nextInt());
					break;
				case STRING:
					currentPage.minipages.get(i).vals.add(scanner.next());
					break;
				}

			}
			
			
			
			scanner.close();
			nTuples++;
			if(nTuples % tuplesPerPage == 0) {
				paxpages.add(currentPage);
				currentPage = new DBPAXpage(schema);
			}
		}
		paxpages.add(currentPage);
		br.close();
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		DBPAXpage rowPage = paxpages.get(rownumber / tuplesPerPage);
		Object[] fields = new Object[schema.length];
		//DataType[] types = new DataType[schema.length];
		
		int offset = rownumber % tuplesPerPage;
		for(int i = 0; i < schema.length; i++) {
			fields[i] = rowPage.minipages.get(i).vals.get(offset);
			//types[i] = rowPage.schema[i];
		}
		
		return new DBTuple(fields, schema);
	}
}
