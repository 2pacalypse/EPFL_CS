package ch.epfl.dias.ops.volcano;


import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	// TODO: Add required structures

	VolcanoOperator child;
	int[] fieldNo;
	
	public Project(VolcanoOperator child, int[] fieldNo) {
		// TODO: Implement
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		DBTuple tuple = child.next();
		if(tuple.eof) {
			return tuple;
		}
		Object[] fields = new Object[fieldNo.length];
		DataType[] types = new DataType[fieldNo.length];
		
		for(int i = 0; i < fieldNo.length; i++) {
			fields[i] = tuple.fields[fieldNo[i]];
			types[i] = tuple.types[fieldNo[i]];
		}
		return new DBTuple(fields, types);
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
}
