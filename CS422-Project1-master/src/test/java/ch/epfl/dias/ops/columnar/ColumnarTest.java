package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class ColumnarTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreData2;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;

	@Before
	public void init() throws IOException {

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",", true);
		columnstoreData.load();
		
		columnstoreData2 = new ColumnStore(schema, "input/data.csv", ",", true);
		columnstoreData2.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|", true);
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|", true);
		columnstoreLineItem.load();
	}

	
//	
//	@Test
//	public void joinTest2() {
//		/*
//		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
//		 * WHERE orderkey = 3;
//		 */
//		
//		long start = System.currentTimeMillis();
//
//		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
//		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
//
//
//		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(scanLineitem, scanOrder, 0, 0);
//		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT,
//				DataType.INT, 0);
//
//		DBColumn[] result = agg.execute();
//
//
//		// This query should return only one result
//		int output = result[0].getAsInteger()[0];
//		long end = System.currentTimeMillis();
//		System.out.println(end - start);
//
//		assertTrue(output == 1000000);
//	}
	
//	@Test
//	public void spTestLINEITEM() {
//		long start = System.currentTimeMillis();
//		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
//		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
//		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.LE, 0, 500000);
//		ch.epfl.dias.ops.columnar.Select sel2 = new ch.epfl.dias.ops.columnar.Select(sel, BinaryOp.EQ, 0, 1);
//		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel2, Aggregate.COUNT,
//				DataType.INT, 0);
//
//		DBColumn[] result = agg.execute();
//
//		// This query should return only one result
//		int output = result[0].getAsInteger()[0];
//		long end = System.currentTimeMillis();
//		System.out.println(end - start);
//		assertTrue(output == 6);
//	}
	
	
	@Test
	public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}
	
	@Test
	public void spTestOrder2() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 0, 1);
		ch.epfl.dias.ops.columnar.Select sel2 = new ch.epfl.dias.ops.columnar.Select(sel, BinaryOp.EQ, 0, 2);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel2, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 0);
	}

	@Test
	public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 0, 1);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 1);
	}

	@Test
	public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];
		
		assertTrue(output == 3);
	}

	@Test
	public void joinTest1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.columnar.Select selOrder = new ch.epfl.dias.ops.columnar.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.columnar.Select selLineitem = new ch.epfl.dias.ops.columnar.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(selOrder, selLineitem, 0, 0);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.columnar.Select selOrder = new ch.epfl.dias.ops.columnar.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.columnar.Select selLineitem = new ch.epfl.dias.ops.columnar.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(selLineitem, selOrder, 0, 0);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}
	
	//trivial sum
	@Test
	public void testAggSumData(){	
		int[] res = new int[] {55, 20, 30, 46, 50, 60, 70, 80, 90, 100};
		for(int i = 0; i < res.length; i++) {
			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, i);
			 DBColumn[] result = agg.execute();
			 int output = result[0].getAsInteger()[0];
				assertTrue(output == res[i]);
		}	
	}
	
	//trivial min
	@Test
	public void testAggMinData(){
		int[] res = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		for(int i = 0; i < res.length; i++) {
			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scan, Aggregate.MIN, DataType.INT, i);
			 DBColumn[] result = agg.execute();
			 int output = result[0].getAsInteger()[0];
				assertTrue(output == res[i]);
		}	
	}
	
	//trivial max
	@Test
	public void testAggMaxData(){
		int[] res = new int[] {10, 2, 3, 6, 5, 6, 7, 8, 9, 10};
		for(int i = 0; i < res.length; i++) {
			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, i);
			 DBColumn[] result = agg.execute();
			 int output = result[0].getAsInteger()[0];
				assertTrue(output == res[i]);
		}	
	}
	
	//trivial avg
	public void testAggAvgData(){
		double[] res = new double[] {5.5, 2.0, 3.0, 4.6, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
		for(int i = 0; i < res.length; i++) {
			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scan, Aggregate.AVG, DataType.INT, i);
			 DBColumn[] result = agg.execute();
			 double output = result[0].getAsDouble()[0];
				assertTrue(output == res[i]);
		}	
	}
	
	//select should return 0
	@Test
	public void testSelectData(){
		int[] res = new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		for(int i = 0; i < res.length; i++) {
			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
			 ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, i, -5);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, i);
			 DBColumn[] result = agg.execute();
			 
			 int output = result[0].getAsInteger()[0];
				assertTrue(output == res[i]);

		}	
	}
	
	// trivial join
	@Test
	public void testJoinData(){

		 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		 ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
	
		 ch.epfl.dias.ops.columnar.Join join = new Join(scan, scan2, 0, 9);
		 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
		 DBColumn[] result = agg.execute();
		 int output = result[0].getAsInteger()[0];
	
			assertTrue(output == 10);
	}
	
	// trivial join2
	@Test
	public void testJoinData5(){

		 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData2);
		 ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
	
		 ch.epfl.dias.ops.columnar.Join join = new Join(scan, scan2, 3, 5);
		 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
		 DBColumn[] result = agg.execute();
		 int output = result[0].getAsInteger()[0];
	
			assertTrue(output == 30);
	}
	
	//left join zero
	@Test
	public void testJoinData2(){

			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData2);
			 ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);

			 ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.NE, 0, 10);
			 
			 
			 ch.epfl.dias.ops.columnar.Join join = new Join(sel, scan2, 0, 9);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
			 DBColumn[] result = agg.execute();
			 int output = result[0].getAsInteger()[0];
	
				assertTrue(output == 0);
	}
	
	//join on the same cs with different scans
	@Test
	public void testJoinData3(){
		 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		 ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData2);

		 
		 ch.epfl.dias.ops.columnar.Join join = new Join(scan, scan2, 0, 9);
		 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.SUM, DataType.INT, 0);
		 DBColumn[] result = agg.execute();
		 //System.out.println(result[0].indices);
		 int output = result[0].getAsInteger()[0];
			assertTrue(output == 100);
	}
	
	//cascaded 2 joins
	@Test
	public void testJoinData4(){
		 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		 ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData2);
		 ch.epfl.dias.ops.columnar.Scan scan3 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData2);
		 
		 ch.epfl.dias.ops.columnar.Join join = new Join(scan, scan2, 0, 9);
		 ch.epfl.dias.ops.columnar.Join join2 = new Join(scan3, join, 9, 0);
		 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join2, Aggregate.COUNT, DataType.INT, 0);
		 DBColumn[] result = agg.execute();
		 //System.out.println(result[0].indices);
		 int output = result[0].getAsInteger()[0];
			assertTrue(output == 100);
	}
	
	//right join zero
	@Test
	public void testJoinData6(){

			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
			 ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);

			 ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.NE, 0, 10);
			 
			 
			 ch.epfl.dias.ops.columnar.Join join = new Join(scan2, sel, 9, 0);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
			 DBColumn[] result = agg.execute();
			 int output = result[0].getAsInteger()[0];
	
				assertTrue(output == 0);
	}

	//join both zero
	@Test
	public void testJoinData7(){

			 ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
			 ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);

			 ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.NE, 0, 10);
			 ch.epfl.dias.ops.columnar.Select sel2 = new ch.epfl.dias.ops.columnar.Select(scan2, BinaryOp.NE, 0, 10);
			 
			 
			 ch.epfl.dias.ops.columnar.Join join = new Join(sel, sel2, 9, 0);
			 ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
			 DBColumn[] result = agg.execute();
			 int output = result[0].getAsInteger()[0];
	
				assertTrue(output == 0);
	}
	
	
	
}