package ch.epfl.dias.store.column;


import java.util.List;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// TODO: Implement
	public DataType type;
	public List<Object> vals;
	
	public List<Integer> indices;
	
	public boolean lateMaterialization;
	
	public DBColumn(DataType type, List<Object> vals) {
		this.type = type;
		this.vals = vals;
		this.indices = null;
	}
	
	public DBColumn(List<Object> vals) {
		this.vals = vals;
	}
	
	
	

	
	
	public Integer[] getAsInteger() {
		// TODO: Implement
		Integer[] ret = vals.toArray(new Integer[0]);
		if(indices == null)
			return ret;
		else {
			Integer[] newRet = new Integer[indices.size()];
			for(int i = 0; i < indices.size();i++) {
				newRet[i] = ret[indices.get(i)];
			}
			return newRet;
		}
	}
	
	public Double[] getAsDouble() {
		
		Double[] ret = vals.toArray(new Double[0]);
		if(indices == null)
			return ret;
		else {
			Double[] newRet = new Double[indices.size()];
			for(int i = 0; i < indices.size();i++) {
				newRet[i] = ret[indices.get(i)];
			}
			return newRet;
		}
	}
	
	public String[] getAsString() {
		return vals.toArray(new String[0]);
	}
	
	public Boolean[] getAsBoolean() {
		return vals.toArray(new Boolean[0]);
	}
}
