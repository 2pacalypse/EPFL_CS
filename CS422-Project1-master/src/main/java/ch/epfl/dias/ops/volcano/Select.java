package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	// TODO: Add required structures
	public VolcanoOperator child;
	public BinaryOp op;
	public int fieldNo;
	public int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
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
	public DBTuple next() {
		// TODO: Implement
		DBTuple selected = child.next();
		while(!selected.eof && !evalPred(op,(int) selected.fields[fieldNo], value)) {
			selected = child.next();
		}
		
		
		return selected;
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
	
	private boolean evalPred(BinaryOp op, int v1, int v2) {
		 //LT,LE,EQ,NE,GT,GE
		 switch(op) {
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
