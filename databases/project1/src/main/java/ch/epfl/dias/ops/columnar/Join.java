package ch.epfl.dias.ops.columnar;


import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class Join implements ColumnarOperator {

	// TODO: Add required structures

	ColumnarOperator leftChild;
	ColumnarOperator rightChild;
	int leftFieldNo;
	int rightFieldNo;
	
	
	
	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {

		// TODO: Implement
		DBColumn[] leftColumns = leftChild.execute();
		DBColumn[] rightColumns = rightChild.execute();
		
		boolean lateMaterializationLeft = leftColumns[0].indices != null;
		boolean lateMaterializationRight = rightColumns[0].indices != null;
		
	
		
		if(!lateMaterializationLeft && !lateMaterializationRight) {
			long start = System.currentTimeMillis();
			HashMap<Object, ArrayList<Integer>> hashtable = new HashMap<Object, ArrayList<Integer>>();
			
			List<Object> leftCol = leftColumns[leftFieldNo].vals;
			
	
			
			for(int i = 0; i < leftCol.size(); i++) {
				if(!hashtable.containsKey(leftCol.get(i))) {
					hashtable.put(leftCol.get(i), new ArrayList<Integer>(Arrays.asList(i)));
				}else {
					hashtable.get(leftCol.get(i)).add(i);
				}
			}
			
			List<List<Object>> newColumns = new ArrayList<List<Object>>();
			for(int i = 0; i < leftColumns.length + rightColumns.length;i++) {
				newColumns.add(new ArrayList<Object>());
			}
			
			List<Object> rightCol = rightColumns[rightFieldNo].vals;
	
			
			for(int i = 0; i < rightCol.size(); i++) {

				
				if(hashtable.containsKey(rightCol.get(i))) {
					List<Integer> rows = hashtable.get(rightCol.get(i));
					for(int row: rows) {
						for(int j = 0; j < leftColumns.length; j++) {
							newColumns.get(j).add(leftColumns[j].vals.get(row));
						}
						for(int j = leftColumns.length; j < newColumns.size(); j++) {
							newColumns.get(j).add(rightColumns[j - leftColumns.length].vals.get(i));
						}
					}

				}
			}
			
			DBColumn[] ret = new DBColumn[newColumns.size()];
			for(int i = 0; i < leftColumns.length; i++) {
				ret[i] = new DBColumn(leftColumns[i].type, newColumns.get(i));
			}
			for(int i = leftColumns.length; i < newColumns.size(); i++) {
				ret[i] = new DBColumn(rightColumns[i - leftColumns.length].type, newColumns.get(i));
			}
			
			
			long end = System.currentTimeMillis();
			System.out.println(end - start);
			return ret;
			
			
			
			
			
		}else if(lateMaterializationLeft && lateMaterializationRight){
			List<List<Integer>> leftIndicesList = new ArrayList<List<Integer>>();
			for(int i = 0; i < leftColumns.length; i++) {
				if(!leftIndicesList.contains(leftColumns[i].indices)) {
					leftIndicesList.add(leftColumns[i].indices);
				}
			}
			
			List<List<Integer>> leftIndicesListCopies = new ArrayList<List<Integer>>();
			for(List<Integer> leftIndices: leftIndicesList) {
				leftIndicesListCopies.add(List.copyOf(leftIndices));
			}
			
			
			List<List<Integer>> rightIndicesList = new ArrayList<List<Integer>>();
			for(int i = 0; i < rightColumns.length; i++) {
				if(!rightIndicesList.contains(rightColumns[i].indices)) {
					rightIndicesList.add(rightColumns[i].indices);
				}
			}
			
			List<List<Integer>> rightIndicesListCopies = new ArrayList<List<Integer>>();
			for(List<Integer> rightIndices: rightIndicesList) {
				rightIndicesListCopies.add(List.copyOf(rightIndices));
			}
			

			
			
			
			HashMap<Object, ArrayList<Integer>> hashtable = new HashMap<Object, ArrayList<Integer>>();
			
			List<Integer> leftColIndices = leftColumns[leftFieldNo].indices;
			Iterator<Integer> lindexIter = leftColIndices.iterator();
			
			for(int i = 0; i < leftColIndices.size(); i++) {
				Integer index = lindexIter.next();
				
				if(!hashtable.containsKey(leftColumns[leftFieldNo].vals.get(index))) {
					hashtable.put(leftColumns[leftFieldNo].vals.get(index), new ArrayList<Integer>(Arrays.asList(i)));
				}else {
					hashtable.get(leftColumns[leftFieldNo].vals.get(index)).add(i);
				}
			}
			
			
			List<Integer> rightColIndices = rightColumns[rightFieldNo].indices;
			Iterator<Integer> rindexIter = rightColIndices.iterator();
				
			List<Integer> leftIndicesOfIndicesToKeep = new ArrayList<Integer>();
			List<Integer> rightIndicesOfIndicesToKeep = new ArrayList<Integer>();
			
			for(int i = 0; i < rightColIndices.size(); i++) {

				Integer index = rindexIter.next();
				
				if(hashtable.containsKey(rightColumns[rightFieldNo].vals.get(index))) {
					List<Integer> indicesOfIndicesLeft = hashtable.get(rightColumns[rightFieldNo].vals.get(index));
					for(int j: indicesOfIndicesLeft) {
						leftIndicesOfIndicesToKeep.add(j);
						rightIndicesOfIndicesToKeep.add(i);
					}
				}
			}
			
			
			
			for(int i = 0; i < leftIndicesList.size(); i++) {
				leftIndicesList.get(i).clear();

				for(int j = 0; j < leftIndicesOfIndicesToKeep.size(); j++) {
					//System.out.println(leftIndicesListCopies.get(i).get(leftIndicesOfIndicesToKeep.get(j)));
					leftIndicesList.get(i).add(leftIndicesListCopies.get(i).get(leftIndicesOfIndicesToKeep.get(j)));			
				}
			}
			
	
			for(int i = 0; i < rightIndicesList.size(); i++) {
				rightIndicesList.get(i).clear();
				for(int j = 0; j < rightIndicesOfIndicesToKeep.size(); j++) {
					rightIndicesList.get(i).add(rightIndicesListCopies.get(i).get(rightIndicesOfIndicesToKeep.get(j)));			
				}
			}

			
			DBColumn[] ret = new DBColumn[leftColumns.length + rightColumns.length];
			for(int i = 0; i < leftColumns.length; i++) {
				ret[i] = leftColumns[i];
				
			}
			for(int i = leftColumns.length; i < leftColumns.length + rightColumns.length; i++) {
				ret[i] = rightColumns[i - leftColumns.length];
			}
			
			return ret;
			
		}else if(!lateMaterializationLeft && lateMaterializationRight) {
			return null;
//			int nRows = leftColumns[0].vals.size();
//			List<Integer> createdIndices = new ArrayList<Integer>();
//			for(int i = 0; i < nRows; i++) {
//				createdIndices.add(i);
//			}
//			for(DBColumn col: leftColumns) {
//				col.indices = createdIndices;
//			}
//			
//		
//			
//			List<List<Integer>> leftIndicesList = new ArrayList<List<Integer>>();
//			for(int i = 0; i < leftColumns.length; i++) {
//				if(!leftIndicesList.contains(leftColumns[i].indices)) {
//					leftIndicesList.add(leftColumns[i].indices);
//				}
//			}
//			
//			List<List<Integer>> leftIndicesListCopies = new ArrayList<List<Integer>>();
//			for(List<Integer> leftIndices: leftIndicesList) {
//				leftIndicesListCopies.add(List.copyOf(leftIndices));
//			}
//			
//			
//			List<List<Integer>> rightIndicesList = new ArrayList<List<Integer>>();
//			for(int i = 0; i < rightColumns.length; i++) {
//				if(!rightIndicesList.contains(rightColumns[i].indices)) {
//					rightIndicesList.add(rightColumns[i].indices);
//				}
//			}
//			
//			List<List<Integer>> rightIndicesListCopies = new ArrayList<List<Integer>>();
//			for(List<Integer> rightIndices: rightIndicesList) {
//				rightIndicesListCopies.add(List.copyOf(rightIndices));
//			}
//			
//
//			
//			
//			
//			HashMap<Object, ArrayList<Integer>> hashtable = new HashMap<Object, ArrayList<Integer>>();
//			
//			List<Integer> leftColIndices = leftColumns[leftFieldNo].indices;
//			Iterator<Integer> lindexIter = leftColIndices.iterator();
//			
//			for(int i = 0; i < leftColIndices.size(); i++) {
//				Integer index = lindexIter.next();
//				
//				if(!hashtable.containsKey(leftColumns[leftFieldNo].vals.get(index))) {
//					hashtable.put(leftColumns[leftFieldNo].vals.get(index), new ArrayList<Integer>(Arrays.asList(i)));
//				}else {
//					hashtable.get(leftColumns[leftFieldNo].vals.get(index)).add(i);
//				}
//			}
//			
//			
//			List<Integer> rightColIndices = rightColumns[rightFieldNo].indices;
//			Iterator<Integer> rindexIter = rightColIndices.iterator();
//				
//			List<Integer> leftIndicesOfIndicesToKeep = new ArrayList<Integer>();
//			List<Integer> rightIndicesOfIndicesToKeep = new ArrayList<Integer>();
//			
//			for(int i = 0; i < rightColIndices.size(); i++) {
//
//				Integer index = rindexIter.next();
//				
//				if(hashtable.containsKey(rightColumns[rightFieldNo].vals.get(index))) {
//					List<Integer> indicesOfIndicesLeft = hashtable.get(rightColumns[rightFieldNo].vals.get(index));
//					for(int j: indicesOfIndicesLeft) {
//						leftIndicesOfIndicesToKeep.add(j);
//						rightIndicesOfIndicesToKeep.add(i);
//					}
//				}
//			}
//			
//			
//			
//			for(int i = 0; i < leftIndicesList.size(); i++) {
//				leftIndicesList.get(i).clear();
//
//				for(int j = 0; j < leftIndicesOfIndicesToKeep.size(); j++) {
//					//System.out.println(leftIndicesListCopies.get(i).get(leftIndicesOfIndicesToKeep.get(j)));
//					leftIndicesList.get(i).add(leftIndicesListCopies.get(i).get(leftIndicesOfIndicesToKeep.get(j)));			
//				}
//			}
//			
//	
//			for(int i = 0; i < rightIndicesList.size(); i++) {
//				rightIndicesList.get(i).clear();
//				for(int j = 0; j < rightIndicesOfIndicesToKeep.size(); j++) {
//					rightIndicesList.get(i).add(rightIndicesListCopies.get(i).get(rightIndicesOfIndicesToKeep.get(j)));			
//				}
//			}
//
//			
//			DBColumn[] ret = new DBColumn[leftColumns.length + rightColumns.length];
//			for(int i = 0; i < leftColumns.length; i++) {
//				ret[i] = leftColumns[i];
//				
//			}
//			for(int i = leftColumns.length; i < leftColumns.length + rightColumns.length; i++) {
//				ret[i] = rightColumns[i - leftColumns.length];
//			}
//			
//			return ret;
//			
//			
//			
//			
//			
//			
//			
//			
//			
//			
			
		}
		else {
			return null;
//			
//			 //lateLEFT && !LATERIGHT
//			int nRows = rightColumns[0].vals.size();
//			List<Integer> createdIndices = new ArrayList<Integer>();
//			for(int i = 0; i < nRows; i++) {
//				createdIndices.add(i);
//			}
//			for(DBColumn col: rightColumns) {
//				col.indices = createdIndices;
//			}
//			
//		
//			
//			List<List<Integer>> leftIndicesList = new ArrayList<List<Integer>>();
//			for(int i = 0; i < leftColumns.length; i++) {
//				if(!leftIndicesList.contains(leftColumns[i].indices)) {
//					leftIndicesList.add(leftColumns[i].indices);
//				}
//			}
//			
//			List<List<Integer>> leftIndicesListCopies = new ArrayList<List<Integer>>();
//			for(List<Integer> leftIndices: leftIndicesList) {
//				leftIndicesListCopies.add(List.copyOf(leftIndices));
//			}
//			
//			
//			List<List<Integer>> rightIndicesList = new ArrayList<List<Integer>>();
//			for(int i = 0; i < rightColumns.length; i++) {
//				if(!rightIndicesList.contains(rightColumns[i].indices)) {
//					rightIndicesList.add(rightColumns[i].indices);
//				}
//			}
//			
//			List<List<Integer>> rightIndicesListCopies = new ArrayList<List<Integer>>();
//			for(List<Integer> rightIndices: rightIndicesList) {
//				rightIndicesListCopies.add(List.copyOf(rightIndices));
//			}
//			
//
//			
//			
//			
//			HashMap<Object, ArrayList<Integer>> hashtable = new HashMap<Object, ArrayList<Integer>>();
//			
//			List<Integer> leftColIndices = leftColumns[leftFieldNo].indices;
//			Iterator<Integer> lindexIter = leftColIndices.iterator();
//			
//			for(int i = 0; i < leftColIndices.size(); i++) {
//				Integer index = lindexIter.next();
//				
//				if(!hashtable.containsKey(leftColumns[leftFieldNo].vals.get(index))) {
//					hashtable.put(leftColumns[leftFieldNo].vals.get(index), new ArrayList<Integer>(Arrays.asList(i)));
//				}else {
//					hashtable.get(leftColumns[leftFieldNo].vals.get(index)).add(i);
//				}
//			}
//			
//			
//			List<Integer> rightColIndices = rightColumns[rightFieldNo].indices;
//			Iterator<Integer> rindexIter = rightColIndices.iterator();
//				
//			List<Integer> leftIndicesOfIndicesToKeep = new ArrayList<Integer>();
//			List<Integer> rightIndicesOfIndicesToKeep = new ArrayList<Integer>();
//			
//			for(int i = 0; i < rightColIndices.size(); i++) {
//
//				Integer index = rindexIter.next();
//				
//				if(hashtable.containsKey(rightColumns[rightFieldNo].vals.get(index))) {
//					List<Integer> indicesOfIndicesLeft = hashtable.get(rightColumns[rightFieldNo].vals.get(index));
//					for(int j: indicesOfIndicesLeft) {
//						leftIndicesOfIndicesToKeep.add(j);
//						rightIndicesOfIndicesToKeep.add(i);
//					}
//				}
//			}
//			
//			
//			
//			for(int i = 0; i < leftIndicesList.size(); i++) {
//				leftIndicesList.get(i).clear();
//
//				for(int j = 0; j < leftIndicesOfIndicesToKeep.size(); j++) {
//					//System.out.println(leftIndicesListCopies.get(i).get(leftIndicesOfIndicesToKeep.get(j)));
//					leftIndicesList.get(i).add(leftIndicesListCopies.get(i).get(leftIndicesOfIndicesToKeep.get(j)));			
//				}
//			}
//			
//	
//			for(int i = 0; i < rightIndicesList.size(); i++) {
//				rightIndicesList.get(i).clear();
//				for(int j = 0; j < rightIndicesOfIndicesToKeep.size(); j++) {
//					rightIndicesList.get(i).add(rightIndicesListCopies.get(i).get(rightIndicesOfIndicesToKeep.get(j)));			
//				}
//			}
//
//			
//			DBColumn[] ret = new DBColumn[leftColumns.length + rightColumns.length];
//			for(int i = 0; i < leftColumns.length; i++) {
//				ret[i] = leftColumns[i];
//				
//			}
//			for(int i = leftColumns.length; i < leftColumns.length + rightColumns.length; i++) {
//				ret[i] = rightColumns[i - leftColumns.length];
//			}
//			
//			return ret;
//			
//			
//			
//			
//			
//			
//			
//			
//			
			
			
		}


	}
}
