package ch.epfl.dias.ops.volcano;


import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures

	public Store store;
	public int rowNumber;
	
	public Scan(Store store) {
		this.store = store;
	}

	@Override
	public void open() {
		// TODO: Implement
		rowNumber = 0;
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		try {
			DBTuple ret = store.getRow(rowNumber);
			rowNumber++;
			return ret;
		}catch (IndexOutOfBoundsException e) {
			return new DBTuple();
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		rowNumber = 0;
		
	}
}