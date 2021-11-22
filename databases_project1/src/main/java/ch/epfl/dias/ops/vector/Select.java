package ch.epfl.dias.ops.vector;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	// TODO: Add required structures

	public VectorOperator child;
	public BinaryOp op;
	public int fieldNo;
	public int value;
	
	
	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
		
	}
	
	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		DBColumn[] columns = child.next();
		if(columns == null) {
			return null;
		}
		
		List<List<Object>> newColumns = new ArrayList<List<Object>>();
		for(int i = 0; i < columns.length;i++) {
			newColumns.add(new ArrayList<Object>());
		}
		
		int nRecords = columns[0].vals.size();
		for(int i = 0; i < nRecords;i++) {


			if(evalPred(op, (int) columns[fieldNo].vals.get(i), value)) {
				for(int k = 0; k < columns.length; k++) {
					newColumns.get(k).add(columns[k].vals.get(i));
				}
			}

			
				
		}
		
		for(int i = 0; i < columns.length; i++) {
			columns[i].vals = newColumns.get(i);
		}
		return columns;

		
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
	
	private boolean evalPred(BinaryOp op, int v1, int v2) {
		switch(op) {
		//LT,LE,EQ,NE,GT,GE
		case LT:
			return v1 < v2;
		case LE:
			return v1 <= v2;
		case EQ:
			return v1 == v2;
		case NE:
			return v1 != v2;
		case GT:
			return v1 > v2;
		case GE:
			return v1 >= v2;
			default:
				return false;
		}
	}
}
