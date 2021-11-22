package ch.epfl.dias.ops.volcano;



import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	// TODO: Add required structures
	VolcanoOperator child;
	Aggregate agg;
	DataType dt; 
	int fieldNo;

	int nElements;
	double min;
	double max;
	double sum;
	
	boolean produced;
	
	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
		
		
		this.produced = false;
		
		
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
		
		nElements = 0;
		sum = 0;
		max = Double.NEGATIVE_INFINITY;
		min = Double.POSITIVE_INFINITY;
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		
		//COUNT,SUM,MIN,MAX,AVG
		if(produced)
			return new DBTuple();
		
		DBTuple currentTuple = child.next();

		
		while(!currentTuple.eof) {
			switch(agg) {
			case COUNT:
				nElements++;
				break;
			case SUM:
				if(dt == DataType.INT)
					sum += currentTuple.getFieldAsInt(fieldNo);
				else
					sum += currentTuple.getFieldAsDouble(fieldNo);
				break;
			case MIN:
				if(dt == DataType.INT && currentTuple.getFieldAsInt(fieldNo) < min)
						min = currentTuple.getFieldAsInt(fieldNo);
				if(dt == DataType.DOUBLE && currentTuple.getFieldAsDouble(fieldNo) < min)
						min = currentTuple.getFieldAsDouble(fieldNo);
				
				break;
			case MAX:
				if(dt == DataType.INT && currentTuple.getFieldAsInt(fieldNo) > max)
					max = currentTuple.getFieldAsInt(fieldNo);
				if(dt == DataType.DOUBLE && currentTuple.getFieldAsDouble(fieldNo) > max)
					max = currentTuple.getFieldAsDouble(fieldNo);
				break;
			case AVG:
				nElements++;
				if(dt == DataType.INT)
					sum += currentTuple.getFieldAsInt(fieldNo);
				else 
					sum += currentTuple.getFieldAsDouble(fieldNo);
			}
			
			currentTuple = child.next();
		}
		
		produced = true;
		switch(agg) {
		case COUNT:
			return new DBTuple(new Object[] {nElements}, new DataType[] {dt});
		case SUM:
			if(dt == DataType.DOUBLE)
				return new DBTuple(new Object[] {sum}, new DataType[] {dt});
			else
				return new DBTuple(new Object[] {(int) sum}, new DataType[] {dt});
		case MIN:
			if(dt == DataType.DOUBLE) {
				return new DBTuple(new Object[] {min}, new DataType[] {dt});
			}else {
				return new DBTuple(new Object[] {(int)min}, new DataType[] {dt});
			}
			
		case MAX:
			if(dt == DataType.DOUBLE)
				return new DBTuple(new Object[] {max}, new DataType[] {dt});
			else
				return new DBTuple(new Object[] {(int) max}, new DataType[] {dt});
			
		case AVG:			
			return new DBTuple(new Object[] {sum / nElements}, new DataType[] {dt});
			default:
				return new DBTuple();
		}
		
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
		
		nElements = 0;
		sum = 0;
		max = Double.NEGATIVE_INFINITY;
		min = Double.POSITIVE_INFINITY;
		
	}

}
