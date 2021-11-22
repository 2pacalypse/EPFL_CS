package ch.epfl.dias.ops.columnar;



import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements ColumnarOperator {

	// TODO: Add required structures

	
	public ColumnStore store;
	
	public Scan(ColumnStore store) {
		// TODO: Implement
		this.store = new ColumnStore(store);
		
	
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		return this.store.columns.toArray(new DBColumn[0]);
		
	}
}
