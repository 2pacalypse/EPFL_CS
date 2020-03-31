package ch.epfl.dias.ops.vector;


import ch.epfl.dias.store.column.DBColumn;


public class Project implements VectorOperator {

	// TODO: Add required structures

	public VectorOperator child;
	public int[] fieldNo;
	
	int nElements;
	double min;
	double max;
	double sum;
	
	
	public Project(VectorOperator child, int[] fieldNo) {
		// TODO: Implemtent
		this.child = child;
		this.fieldNo = fieldNo;
		
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		
		
		DBColumn[] childColumns = child.next();
		if(childColumns == null)
			return null;

		DBColumn[] columnsFetched = new DBColumn[fieldNo.length];
		for(int i = 0; i < fieldNo.length; i++) 
			columnsFetched[i] = childColumns[fieldNo[i]];
		return columnsFetched;

	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
