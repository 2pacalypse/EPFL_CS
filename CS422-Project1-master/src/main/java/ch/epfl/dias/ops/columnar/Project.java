package ch.epfl.dias.ops.columnar;


import ch.epfl.dias.store.column.DBColumn;

public class Project implements ColumnarOperator {

	// TODO: Add required structures

	public ColumnarOperator child;
	int[] columns;
	
	public Project(ColumnarOperator child, int[] columns) {
		// TODO: Implement
		this.child = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		// TODO: Implement
		
		
		DBColumn[] childColumns = child.execute();

		DBColumn[] columnsFetched = new DBColumn[columns.length];
		for(int i = 0; i < columns.length; i++) 
			columnsFetched[i] = childColumns[columns[i]];
		return columnsFetched;
				
		
				
		
	}
}
