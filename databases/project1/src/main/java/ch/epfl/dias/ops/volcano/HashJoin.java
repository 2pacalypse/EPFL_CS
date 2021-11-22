package ch.epfl.dias.ops.volcano;


import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class HashJoin implements VolcanoOperator {

	// TODO: Add required structures
	VolcanoOperator leftChild;
	VolcanoOperator rightChild;
	int leftFieldNo;
	int rightFieldNo;
	
	Iterator<DBTuple> onGoingLeftChild;
	DBTuple onGoingRightChild;
	HashMap<Object, ArrayList<DBTuple>> hashtable;
	
	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.hashtable = new HashMap<Object, ArrayList<DBTuple>>();
		this.onGoingLeftChild = null;
		this.onGoingRightChild = null;
	}

	@Override
	public void open() {
		// TODO: Implement
		leftChild.open();
		rightChild.open();
		
		DBTuple currentTuple = leftChild.next();
		while(!currentTuple.eof) {
			if(!hashtable.containsKey(currentTuple.fields[leftFieldNo])) {
				hashtable.put(currentTuple.fields[leftFieldNo], new ArrayList<DBTuple>(Arrays.asList(currentTuple)));	
			}else {
				hashtable.get(currentTuple.fields[leftFieldNo]).add(currentTuple);
			}
			
			currentTuple = leftChild.next();
		}
		
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		

		if(onGoingLeftChild == null || !onGoingLeftChild.hasNext()) {
			onGoingLeftChild = null;
			
			onGoingRightChild = rightChild.next();
			while(!onGoingRightChild.eof && !hashtable.containsKey(onGoingRightChild.fields[rightFieldNo])) {
				onGoingRightChild = rightChild.next();
			}
			if(onGoingRightChild.eof) {
				return onGoingRightChild;
			}	
			List<DBTuple> leftTuples = hashtable.get(onGoingRightChild.fields[rightFieldNo]);
			onGoingLeftChild = leftTuples.iterator();
			
			DBTuple leftTuple = onGoingLeftChild.next();
			DBTuple rightTuple = onGoingRightChild;
			
			Object[] bothfields= Arrays.copyOf(leftTuple.fields, leftTuple.fields.length + rightTuple.fields.length);
			System.arraycopy(rightTuple.fields, 0, bothfields, leftTuple.fields.length, rightTuple.fields.length);
			
			DataType[] bothdts= Arrays.copyOf(leftTuple.types, leftTuple.types.length + rightTuple.types.length);
			System.arraycopy(rightTuple.types, 0, bothdts, leftTuple.types.length, rightTuple.types.length);
			
			return new DBTuple(bothfields, bothdts);
		}
			//ongoing left child is not null an has next.
		
		DBTuple leftTuple = onGoingLeftChild.next();
		DBTuple rightTuple = onGoingRightChild;
		
		Object[] bothfields= Arrays.copyOf(leftTuple.fields, leftTuple.fields.length + rightTuple.fields.length);
		System.arraycopy(rightTuple.fields, 0, bothfields, leftTuple.fields.length, rightTuple.fields.length);
		
		DataType[] bothdts= Arrays.copyOf(leftTuple.types, leftTuple.types.length + rightTuple.types.length);
		System.arraycopy(rightTuple.types, 0, bothdts, leftTuple.types.length, rightTuple.types.length);
		
		return new DBTuple(bothfields, bothdts);
			
				
	}

	@Override
	public void close() {
		// TODO: Implement
		leftChild.close();
		rightChild.close();
	}
}
