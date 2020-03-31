package ch.epfl.dias.ops.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;

import ch.epfl.dias.store.column.DBColumn;


public class ProjectAggregate implements VectorOperator {

	// TODO: Add required structures
	
	VectorOperator child;
	Aggregate agg;
	DataType dt;
	int fieldNo;
	
	int nElements;
	double min;
	double max;
	double sum;
	
	public boolean produced;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
		

	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
		
		nElements = 0;
		sum = 0;
		max = Double.NEGATIVE_INFINITY;
		min = Double.POSITIVE_INFINITY;
		
		produced = false;
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement

		
		if(produced)
			return null;
		
		DBColumn[] columns = child.next();
		
		while(columns != null) {
		Iterator<Object> iter= columns[fieldNo].vals.iterator();
		for(int i = 0; i < columns[fieldNo].vals.size();i++) {
			Object o = iter.next();
			switch(agg) {
			case COUNT:
				nElements++;
				break;
			case SUM:
				if(dt == DataType.DOUBLE)
					sum += (double) o;
				else
					sum += (int)o;
				break;
			case MIN:
				
				if(dt == DataType.DOUBLE && (double) o < min) {
					min = (double) o;
				}
				if(dt == DataType.INT && (int) o < min) {
					min = (int) o;
				}
				break;
			case MAX:
				if(dt == DataType.DOUBLE && (double) o > max) {
					max = (double) o;
				}
				if(dt == DataType.INT && (int) o > max) {
					max = (int) o;
				}
				break;
			case AVG:
				nElements++;
				if(dt == DataType.DOUBLE)
				sum += (double) o;
				else
					sum += (int) o;
				break;
			}
		}
		columns = child.next();
		}
		
		produced = true;
		switch(agg) {
		case COUNT:
			if(dt == DataType.DOUBLE)
				return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList(nElements)))};
			else
				return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList((int)nElements)))};
		case SUM:
			if(dt == DataType.DOUBLE)
			return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList(sum)))};
			else
				return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList((int)sum)))};
		case MIN:
			if(dt == DataType.DOUBLE)
			return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList(min)))};
			else
				return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList((int)min)))}; 
		case MAX:
			if(dt == DataType.DOUBLE)
			return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList(max)))};
			else
				return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList((int)max)))};
		case AVG:
			return new DBColumn[] {new DBColumn(dt, new ArrayList<Object>(Arrays.asList(sum/nElements)))};
			default:
				return columns;
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}

}
