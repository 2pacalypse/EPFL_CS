package ch.epfl.dias.ops.vector;


import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class Join implements VectorOperator {

	// TODO: Add required structures
	
	VectorOperator leftChild;
	VectorOperator rightChild;
	int leftFieldNo;
	int rightFieldNo;
	
	Iterator<DBTuple> onGoingLeftChild;
	DBColumn[] onGoingRightChild;
	HashMap<Object, ArrayList<DBTuple>> hashtable;

	public int leftColSize;
	public int rightColSize;
	
	List<List<Object>> cache;
	
	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.hashtable = new HashMap<Object, ArrayList<DBTuple>>();

		
	}

	@Override
	public void open() {
		// TODO: Implement
		leftChild.open();
		rightChild.open();
		
		
//		List<List<Object>> newColumns = new ArrayList<List<Object>>();
//		for(int i = 0; i < leftColSize + onGoingRightChild.length;i++) {
//			newColumns.add(new LinkedList<Object>());
//		}

		
		DBColumn[] leftChildColumns = leftChild.next();
		
		while(leftChildColumns != null) {
			leftColSize = leftChildColumns.length;
			for(int i = 0; i < leftChildColumns[0].vals.size(); i++) {
				Object[] objs = new Object[leftChildColumns.length];
				
				for(int j = 0; j < leftChildColumns.length; j++) {
					objs[j] = leftChildColumns[j].vals.get(i);
				}
				DBTuple currentTuple = new DBTuple(objs);
				
				if(!hashtable.containsKey(currentTuple.fields[leftFieldNo])) {
					hashtable.put(currentTuple.fields[leftFieldNo], new ArrayList<DBTuple>(Arrays.asList(currentTuple)));	
				}else {
					hashtable.get(currentTuple.fields[leftFieldNo]).add(currentTuple);
				}
			}
			
			leftChildColumns = leftChild.next();
		}
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		
		if(cache == null) {
			DBColumn[] rightColumns = rightChild.next();
			if(rightColumns == null) {
				return null;
			}
			
			rightColSize = rightColumns.length;
			
			cache = new ArrayList<List<Object>>();
			for(int i = 0; i < leftColSize + rightColSize;i++) {
				cache.add(new ArrayList<Object>());
			}
			
			for(int i = 0; i < rightColumns[0].vals.size();i++) {
				if(hashtable.containsKey(rightColumns[rightFieldNo].vals.get(i))) {
					List<DBTuple> leftTuples = hashtable.get(rightColumns[rightFieldNo].vals.get(i));
					for(DBTuple row: leftTuples) {
						for(int j = 0; j < row.fields.length; j++)
							cache.get(j).add(row.fields[j]);
						for(int j = leftColSize; j < leftColSize + rightColSize; j++) {
							cache.get(j).add(rightColumns[j - leftColSize].vals.get(i));
						}
					}
				}
			}
		}
				

			DBColumn[] ret = new DBColumn[leftColSize + rightColSize];
			for(int i = 0; i < ret.length; i++) {
				
				List<Object> vals = cache.get(i).subList(0, rightColSize < cache.get(i).size() ? rightColSize: cache.get(i).size() );
				ret[i] = new DBColumn(new ArrayList<Object>(vals));
				vals.clear();
			}
			
			if(cache.get(0).size() == 0)
				cache = null;
			
			return ret;

	}
	

	@Override
	public void close() {
		// TODO: Implement
		leftChild.close();
		rightChild.close();
	}
}
