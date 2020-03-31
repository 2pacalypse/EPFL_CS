package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements ColumnarOperator {

	// TODO: Add required structures
	ColumnarOperator child;
	Aggregate agg;
	DataType dt;
	int fieldNo;
	
	int nElements;
	double min;
	double max;
	double sum;
	
	boolean produced;
	
	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
		
		nElements = 0;
		sum = 0;
		max = Double.NEGATIVE_INFINITY;
		min = Double.POSITIVE_INFINITY;
		
		produced = false;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		
		if(produced)
			return null;
		
		DBColumn[] columns = child.execute();
		boolean lateMaterialization = (columns[0].indices != null);
		

		//COUNT,SUM,MIN,MAX,AVG
		if(lateMaterialization) {
			
			Iterator<Integer> iter = columns[fieldNo].indices.iterator();
			
			for(int i = 0; i < columns[fieldNo].indices.size();i++) {
//				if(i % 10000 == 0) {
//					long endTime = System.currentTimeMillis();
//					long duration = (endTime - startTime); 
//					System.out.println(duration );
//					startTime = endTime;
//				}
				 
				int index = iter.next();
				switch(agg) {
				case COUNT:
					nElements++;
					break;
				case SUM:
					if(dt == DataType.DOUBLE)
					sum += (double) columns[fieldNo].vals.get(index);
					else
						sum += (int) columns[fieldNo].vals.get(index);	
					break;
				case MIN:
					if(dt == DataType.DOUBLE && (double) columns[fieldNo].vals.get(index) < min) {
						min = (double) columns[fieldNo].vals.get(index);
					}
					if(dt == DataType.INT && (int) columns[fieldNo].vals.get(index) < min) {
						min = (int) columns[fieldNo].vals.get(index);
					}
					break;
				case MAX:
					if(dt == DataType.DOUBLE && (double) columns[fieldNo].vals.get(index) > max) {
						max = (double) columns[fieldNo].vals.get(index);
					}
					if(dt == DataType.INT && (int) columns[fieldNo].vals.get(index) > max) {
						max = (int) columns[fieldNo].vals.get(index);
					}
					break;
				case AVG:
					nElements++;
					sum += (double) columns[fieldNo].vals.get(index);
					break;
				}
			}
		}else {
			
			Iterator<Object> iter = columns[fieldNo].vals.iterator();
			for(int i = 0; i < columns[fieldNo].vals.size();i++) {
				Object val = iter.next();
				switch(agg) {
				case COUNT:
					nElements++;
					break;
				case SUM:
					if(dt == DataType.DOUBLE)
						sum += (double) val;
					else
						sum += (int)val;
					break;
				case MIN:
					
					if(dt == DataType.DOUBLE && (double) val < min) {
						min = (double) val;
					}
					if(dt == DataType.INT && (int) val < min) {
						min = (int) val;
					}
					break;
				case MAX:
					if(dt == DataType.DOUBLE && (double) val > max) {
						max = (double) val;
					}
					if(dt == DataType.INT && (int) val > max) {
						max = (int) val;
					}
					break;
				case AVG:
					nElements++;
					if(dt == DataType.DOUBLE)
					sum += (double) val;
					else
						sum += (int) val;
					break;
				}
			}
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
}
