package ch.epfl.dias.ops.vector;

import java.io.IOException;
import java.util.Arrays;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.columnar.Join;
import ch.epfl.dias.ops.volcano.HashJoin;
import ch.epfl.dias.ops.volcano.ProjectAggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

public class VectorTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;
	ColumnStore columnstoreEmpty;

	Double[] orderCol3 = { 55314.82, 66219.63, 270741.97, 41714.38, 122444.33, 50883.96, 287534.80, 129634.85,
			126998.88, 186600.18 };
	int numTuplesData = 11;
	int numTuplesOrder = 10;
	int standardVectorsize = 3;

	// 1 seconds max per method tested
	//@Rule
	//public Timeout globalTimeout = Timeout.seconds(1);

	@Before
	public void init() throws IOException {

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		columnstoreLineItem.load();

		columnstoreEmpty = new ColumnStore(schema, "input/empty.csv", ",");
		columnstoreEmpty.load();
	}
	
	
//	@Test
//	public void spJOINITEM() {
//		
//    	long start = System.currentTimeMillis();
//		ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
//		ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
//	
//	
//	    ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scanOrder,scanLineitem,0,0);
//	    ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
//	
//	    agg.open();
//	    //This query should return only one result
//	    DBColumn[] result = agg.next();
//	    int output = result[0].getAsInteger()[0];
//	    long end = System.currentTimeMillis();
//	    System.out.println(end - start);
//	    assertEquals(output, 1000000);
//	}

	
		@Test
		public void spTestData() {
			long start = System.currentTimeMillis();
			/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
			ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 1);
			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
					Aggregate.COUNT, DataType.INT, 0);
	
			agg.open();
			DBColumn[] result = agg.next();
	
			
			// This query should return only one result
			int output = result[0].getAsInteger()[0];
			long end = System.currentTimeMillis();
			System.out.println(end - start);
			assertTrue(output == 6);
		}
	
//	@Test
//	public void spTestData() {
//		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
//				Aggregate.COUNT, DataType.INT, 2);
//
//		agg.open();
//		DBColumn[] result = agg.next();
//
//		
//		// This query should return only one result
//		int output = result[0].getAsInteger()[0];
//		assertTrue(output == 3);
//	}
//
//	@Test
//	public void spTestOrder2() {
//		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 1);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
//				Aggregate.COUNT, DataType.INT, 2);
//
//		agg.open();
//		DBColumn[] result = agg.next();
//
//		// This query should return only one result
//		int output = result[0].getAsInteger()[0];
//		//System.out.println(output);
//		assertTrue(output == 1);
//	}
	
	//	@Test
	//	public void spTestOrder() {
	//		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
	//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
	//		//ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 1);
	//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan,
	//				Aggregate.COUNT, DataType.INT, 2);
	//
	//		agg.open();
	//		DBColumn[] result = agg.next();
	//
	//		// This query should return only one result
	//		int output = result[0].getAsInteger()[0];
	//		//System.out.println(output);
	//		assertTrue(output == 250000);
	//	}
	//	
//	
//	@Test
//	public void spTestLineItem() {
//		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
//				Aggregate.COUNT, DataType.INT, 2);
//
//		agg.open();
//		DBColumn[] result = agg.next();
//
//		// This query should return only one result
//		int output = result[0].getAsInteger()[0];
//		//System.out.println(output);
//		assertTrue(output == 3);
//	}
//	
//	// trivial sum
//	@Test
//	public void testAggSumData(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//		int[] res = new int[] {55, 20, 30, 46, 50, 60, 70, 80, 90, 100};
//		for(int i = 0; i < res.length; i++) {
//			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, i);
//			agg.open();
//			DBColumn[] result = agg.next();
//			int output = result[0].getAsInteger()[0];
//			assertTrue(output == res[i]);
//		}	
//	}
//
//	//trivial min
//	@Test
//	public void testAggMinData(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//		int[] res = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
//		for(int i = 0; i < res.length; i++) {
//			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.INT, i);
//			agg.open();
//			DBColumn[] result = agg.next();
//			int output = result[0].getAsInteger()[0];
//			assertTrue(output == res[i]);
//		}	
//	}
//
//	//trivial max
//	@Test
//	public void testAggMaxData(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//		int[] res = new int[] {10, 2, 3, 6, 5, 6, 7, 8, 9, 10};
//		for(int i = 0; i < res.length; i++) {
//			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, i);
//			agg.open();
//			DBColumn[] result = agg.next();
//			int output = result[0].getAsInteger()[0];
//			assertTrue(output == res[i]);
//		}	
//	}
//
//	//trivial avg
//	@Test
//	public void testAggAvgData(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//		double[] res = new double[] {5.5, 2.0, 3.0, 4.6, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
//		for(int i = 0; i < res.length; i++) {
//			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.INT, i);
//			agg.open();
//			DBColumn[] result = agg.next();
//			double output = result[0].getAsDouble()[0];
//			assertTrue(output == res[i]);
//			agg.close();
//		}	
//	}
//
//	//select returns zero
//	@Test
//	public void testSelectData(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//		int[] res = new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
//		for(int i = 0; i < res.length; i++) {
//			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//			ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, i, -5);
//			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, i);
//			agg.open();
//			DBColumn[] result = agg.next();
//			int output = result[0].getAsInteger()[0];
//			assertTrue(output == res[i]);
//			agg.close();
//		}	
//	}
//
//	
//	//trivial join
//	@Test
//	public void testJoinData(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//
//		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan, scan2, 0, 9);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];
//
//		assertTrue(output == 10);
//	}
//
//	//left join zero
//	@Test
//	public void testJoinData2(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.NE, 0, 10);
//
//
//		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(sel, scan2, 0, 9);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];
//
//		assertTrue(output == 0);
//	}
//
//	//join values
//	@Test
//	public void testJoinData3(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//
//
//		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan, scan2, 0, 9);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.SUM, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];
//		assertTrue(output == 100);
//	}
//
//	
//	//right join zero
//	@Test
//	public void testJoinData4(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.NE, 0, 10);
//
//
//		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan2, sel, 9, 0);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];
//
//		assertTrue(output == 0);
//	}
//
//	//both join zero
//	@Test
//	public void testJoinData5(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
//		
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.NE, 0, 10);
//		ch.epfl.dias.ops.vector.Select sel2 = new ch.epfl.dias.ops.vector.Select(scan2, BinaryOp.NE, 0, 10);
//
//
//		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(sel, sel2, 9, 0);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];
//
//		assertTrue(output == 0);
//	}
//	
//	@Test
//	public void testProject(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
//
//		ch.epfl.dias.ops.vector.Project proj = new Project(scan, new int[] {0});
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(proj, BinaryOp.EQ, 0, 1);
//
//
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];
//
//		assertTrue(output == 6);
//	}
//	
//	@Test
//	public void testProject2(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
//
//		ch.epfl.dias.ops.vector.Project proj = new Project(scan, new int[] {1});
//		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(proj, BinaryOp.EQ, 0, 673091);
//
//
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];
//		assertTrue(output == 1);
//	}
//	
//	@Test
//	public void testJoinData6(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//
//		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
//		ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
//
//		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan, scan2, 0, 0);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.SUM, DataType.INT, 0);
//		agg.open();
//		DBColumn[] result = agg.next();
//		int output = result[0].getAsInteger()[0];	
//		assertTrue(output == 65);
//	}
//	
//	@Test
//	public void testAggMaxDataDBL(){
//		/* SELECT COUNT(*) FROM data WHERE col9 == 10 */	
//			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
//			Select sel = new Select(scan, BinaryOp.LT, 0, 3);
//			Select sel2 = new Select(sel, BinaryOp.LT, 0, 2);
//			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel2, Aggregate.MAX, DataType.DOUBLE, 5);
//			agg.open();
//			DBColumn[] result = agg.next();
//			double output = result[0].getAsDouble()[0];
//			
//			assertTrue(output == 44842.88);
//	}

}
