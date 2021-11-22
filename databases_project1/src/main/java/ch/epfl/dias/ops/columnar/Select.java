package ch.epfl.dias.ops.columnar;

// AFTER PROJECTION WHAT HAPPENS TO FIELDNO

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements ColumnarOperator {

	// TODO: Add required structures

	ColumnarOperator child;
	BinaryOp op;
	int fieldNo;
	int value;
	
	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
		
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] columns = child.execute();
		boolean lateMaterialization = (columns[0].indices != null);
		
		if(lateMaterialization) {
			
			//gather all indices
			List<List<Integer>> allIndices = new ArrayList<List<Integer>>();
			allIndices.add(columns[fieldNo].indices);
			for(DBColumn col: columns) 
				if(!allIndices.contains(col.indices))
					allIndices.add(col.indices);
			
			
			//this will store new indices
			List<List<Integer>> newIndices = new ArrayList<List<Integer>>();
			for(int i = 0; i < allIndices.size(); i++) {
				newIndices.add(new ArrayList<Integer>());
			}
			
			
			
			int nRecords = columns[fieldNo].indices.size();
			for(int i = 0; i < nRecords;i++) {
				int index = columns[fieldNo].indices.get(i);
				//for each index pointer
				//fill newIndices

				if(evalPred(op, (int) columns[fieldNo].vals.get(index), value))
					for(int j = 0; j < newIndices.size(); j++)
						newIndices.get(j).add(allIndices.get(j).get(i));		
			}
			
			for(DBColumn col: columns) {
				for(int i = 0; i < allIndices.size(); i++) {
					if(col.indices == allIndices.get(i)) {
						col.indices = newIndices.get(i);
					}
				}
			}
			return columns;
			
		}else {
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
		
		
		// TODO: Implement
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
