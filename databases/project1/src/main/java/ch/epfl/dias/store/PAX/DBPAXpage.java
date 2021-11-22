package ch.epfl.dias.store.PAX;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
//import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

	// TODO: Implement
	public List<DBColumn> minipages;
	
	public DataType[] schema;
	
	public DBPAXpage(DataType[] schema) {
		this.schema = schema;
		this.minipages = new ArrayList<DBColumn>();
		for(DataType dt : schema) {
			minipages.add(new DBColumn(dt, new ArrayList<Object>()));
		}
	}
	
	
	
}
