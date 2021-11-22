package ch.epfl.dias.ops.vector;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	// TODO: Add required structures

	public Store store;
	public int vectorsize;
	public int rowNumber;
	public DBColumn[] columns;
	
	public Scan(Store store, int vectorsize) {
		// TODO: Implement
		this.store = store;
		this.vectorsize = vectorsize;
		
	}
	
	@Override
	public void open() {
		// TODO: Implement
		rowNumber = 0;
		this.columns = store.getColumns(null);

	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement

		if(rowNumber > columns[0].vals.size()) {
			return null;
		}
		
		DBColumn[] ret = new DBColumn[columns.length];
		for(int i = 0; i < columns.length; i++) {
			
			List<Object> vals = columns[i].vals.subList(rowNumber, rowNumber + vectorsize < columns[i].vals.size() ? rowNumber + vectorsize: columns[i].vals.size() );
			ret[i] = new DBColumn(columns[i].type, new ArrayList<Object>(vals));
		}
		rowNumber += vectorsize;
		return ret;
		
		
	}

	@Override
	public void close() {
		// TODO: Implement
		rowNumber = 0;
	}
}
