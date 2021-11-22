package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	// TODO: Add required structures
	
	
	public List<DBColumn> columns;
	public DataType[] schema;
	public String filename;
	public String delimiter;
	public boolean lateMaterialization;
	
	public int lineCount;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}
	
	
	
	public ColumnStore(ColumnStore s2) {
		this.lateMaterialization = s2.lateMaterialization;
		this.lineCount = s2.lineCount;
		
		this.columns = new ArrayList<DBColumn>();
		for(DBColumn s2col: s2.columns) {
			DBColumn newCol = new DBColumn(s2col.type,s2col.vals);
			columns.add(newCol);
		}
		
		if(lateMaterialization) {
			List<Integer> indices = new ArrayList<Integer>();
			for(int i = 0; i < lineCount; i++) {
				indices.add(i);
			}
			for(DBColumn col: columns) {
				col.indices = indices;
			}
		}
	
	}
	

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.lateMaterialization = lateMaterialization;
		this.columns = new ArrayList<DBColumn>();
		
		if(lateMaterialization) {
			for(DataType dt : schema) {
				columns.add(new DBColumn(dt, new ArrayList<Object>()));
			}	
		}else {
			for(DataType dt : schema) {
				columns.add(new DBColumn(dt, new ArrayList<Object>()));
			}	
		}
		
		lineCount = 0;
		
		
	}

	@Override
	public void load()  {
		// TODO: Implement
		String workingDir = System.getProperty("user.dir");

        Path filePath = Paths.get(workingDir + File.separator + filename);
		
        
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath.toFile()));
			String line;
			while((line = br.readLine()) != null) {
				lineCount++;
				Scanner scanner = new Scanner(line);
				scanner.useDelimiter(delimiter);
				
				
				for(int i = 0; i < schema.length; i++) {
					switch (schema[i]){
					case BOOLEAN:	
						columns.get(i).vals.add(scanner.nextBoolean());
						break;
					case DOUBLE:
						columns.get(i).vals.add(scanner.nextDouble());
						break;
					case INT:
						columns.get(i).vals.add(scanner.nextInt());
						break;
					case STRING:
						columns.get(i).vals.add(scanner.next());
						break;
					}
				}
				
				
				scanner.close();
			}
			

		
			if(lateMaterialization) {
				List<Integer> indices = new ArrayList<Integer>();
				for(int i = 0; i < lineCount; i++) {
					indices.add(i);
				}
				for(DBColumn col: columns) {
					col.indices = indices;
				}
			}
			
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		// TODO: Implement
	
		List<DBColumn> columnsFetched = new ArrayList<DBColumn>();
		if(columnsToGet == null) {
			for(int i = 0; i < columns.size(); i++) {
				columnsFetched.add(columns.get(i));
			}	
		}else {
			for(int i = 0; i < columnsToGet.length; i++) {
				columnsFetched.add(columns.get(columnsToGet[i]));
			}
		}

		return columnsFetched.toArray(new DBColumn[0]);
	}
}
