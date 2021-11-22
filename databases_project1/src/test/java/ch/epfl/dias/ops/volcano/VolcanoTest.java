package ch.epfl.dias.ops.volcano;

import static org.junit.Assert.*;

import java.io.IOException;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;
import ch.epfl.dias.store.PAX.PAXStore;

import org.junit.Before;
import org.junit.Test;

public class VolcanoTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    
    RowStore rowstoreData;
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;
    
    PAXStore rowstoreLineItempax;
    PAXStore rowstoreOrderpax;
    
    @Before
    public void init() throws IOException  {
    	
		schema = new DataType[]{ 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT,
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT };
    	
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
//        rowstoreOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
//        rowstoreOrder.load();
//        
//        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
//        rowstoreLineItem.load();  
        
        rowstoreLineItempax = new PAXStore(lineitemSchema, "input/lineitem_small.csv", "\\|", 1000);
        rowstoreLineItempax.load();
        
        rowstoreOrderpax = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 10000);
        rowstoreOrderpax.load();
    }
    
//  @Test
//  public void spTestLINEITEM(){
//	  long start = System.currentTimeMillis();
//  	/* SELECT COUNT(*) FROM LINEITEM WHERE col0 == 1 */	    
//  	ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItempax);
//  	//ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 1);
//  	ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, 0);
//
//  	agg.open();
//
//  	// This query should return only one result
//  	DBTuple result = agg.next();
//  	 
//  	int output = result.getFieldAsInt(0);
//  	long end = System.currentTimeMillis();
//  	//assertTrue(output == 6);
//  	System.out.println(end - start);
//  	assertEquals(output, 99939);
//  	
//  }
//    
    
    
//    @Test
//  public void jOINTestLINEITEM(){  
//    	long start = System.currentTimeMillis();
//		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrderpax);
//		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItempax);
//	
//	
//	    HashJoin join = new HashJoin(scanOrder,scanLineitem,0,0);
//	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
//	
//	    agg.open();
//	    //This query should return only one result
//	    DBTuple result = agg.next();
//	    int output = result.getFieldAsInt(0);
//	    long end = System.currentTimeMillis();
//	    System.out.println(end - start);
//	    assertEquals(output, 1000000);
//  }
    
    
    
//  @Test
//  public void spTestLINEITEM(){
//	  long start = System.currentTimeMillis();
//  	/* SELECT COUNT(*) FROM LINEITEM WHERE col0 == 1 */	    
//  	ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
//  	ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 1);
//  	ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 0);
//
//  	agg.open();
//
//  	// This query should return only one result
//  	DBTuple result = agg.next();
//  	 
//  	int output = result.getFieldAsInt(0);
//  	long end = System.currentTimeMillis();
//  	assertTrue(output == 6);
//  	System.out.println(end - start);
//  }
    
    
//    @Test
//    public void spTestData(){
//    	/* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
//    	ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//    	ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 6);
//    	ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
//
//    	agg.open();
//
//    	// This query should return only one result
//    	DBTuple result = agg.next();
//    	int output = result.getFieldAsInt(0);
//    	assertTrue(output == 3);
//    }
//	
//	@Test
//	public void spTestDataZeroRows(){
//	    // MAX RETURNS INT.MIN ON 0 ROWS
//		// MIN RETURNS INT.MAX ON 0 ROWS
//		// AVG THROWS EXCEPTION AS 0/0 IS DOUBLE
//		// SUM RETURNS 0
//		// COUNT RETURNS 0
//	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 1);
//	    ch.epfl.dias.ops.volcano.Select sel2 = new ch.epfl.dias.ops.volcano.Select(sel, BinaryOp.EQ, 0, 2);
//	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel2, Aggregate.SUM, DataType.INT, 0);
//	
//		agg.open();
//		
//		DBTuple result = agg.next();
//		int output = result.getFieldAsInt(0);
//		assertEquals(0, output);
//		assertTrue(output == 0);
//	}
//	
//	@Test
//	public void spTestLineItem(){
//	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
//	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
//	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 3);
//	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
//	
//		agg.open();
//		
//		// This query should return only one result
//		DBTuple result = agg.next();
//		int output = result.getFieldAsInt(0);
//		assertTrue(output == 3);
//	}
//
//	@Test
//	public void joinTest1(){
//	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
//	
//		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
//		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
//	
//	    /*Filtering on both sides */
//	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
//	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
//	
//	    HashJoin join = new HashJoin(selOrder,selLineitem,0,0);
//	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
//	
//	    agg.open();
//	    //This query should return only one result
//	    DBTuple result = agg.next();
//	    int output = result.getFieldAsInt(0);
//	    assertTrue(output == 3);
//	}
//	
//	@Test
//	public void joinTest2(){
//	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
//	
//		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
//		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
//	
//	    /*Filtering on both sides */
//	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
//	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
//	
//
//	    HashJoin join = new HashJoin(selLineitem,selOrder,0,0);
//
//	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
//	
//	    agg.open();
//	    //This query should return only one result
//	    DBTuple result = agg.next();
//	    int output = result.getFieldAsInt(0);
//	    
//	    assertTrue(output == 3);
//	}
//    
//    
//	@Test
//	public void testAggSumData(){
//	    // trivial sum
//		int[] res = new int[] {55, 20, 30, 46, 50, 60, 70, 80, 90, 100};
//		for(int i = 0; i < res.length; i++) {
//			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, i);
//			 agg.open();
//			 DBTuple result = agg.next();
//			 int output = result.getFieldAsInt(0);
//				assertTrue(output == res[i]);
//		}	
//	}
//		
//	@Test
//	public void testAggMinData(){
//	    // trivial min	
//		int[] res = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
//		for(int i = 0; i < res.length; i++) {
//			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MIN, DataType.INT, i);
//			 agg.open();
//			 DBTuple result = agg.next();
//			 int output = result.getFieldAsInt(0);
//				assertTrue(output == res[i]);
//			agg.close();
//		}	
//	}
//	
//	@Test
//	public void testAggMaxData(){
//	    //trivial max
//		int[] res = new int[] {10, 2, 3, 6, 5, 6, 7, 8, 9, 10};
//		for(int i = 0; i < res.length; i++) {
//			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, i);
//			 agg.open();
//			 DBTuple result = agg.next();
//			 int output = result.getFieldAsInt(0);
//				assertTrue(output == res[i]);
//			agg.close();
//		}	
//	}
//	
//	@Test
//	public void testAggAvgData(){
//	    //trivial avg	
//		double[] res = new double[] {5.5, 2.0, 3.0, 4.6, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
//		for(int i = 0; i < res.length; i++) {
//			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.AVG, DataType.INT, i);
//			 agg.open();
//			 DBTuple result = agg.next();
//			 
//			 double output = result.getFieldAsDouble(0);
//				assertTrue(output == res[i]);
//			agg.close();
//		}	
//	}
//	
//	@Test
//	public void testSelectData(){
//	    // select should return 0	
//		int[] res = new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
//		for(int i = 0; i < res.length; i++) {
//			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//			 ch.epfl.dias.ops.volcano.Select sel = new Select(scan, BinaryOp.EQ, i, -5);
//			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, i);
//			 agg.open();
//			 DBTuple result = agg.next();
//			 int output = result.getFieldAsInt(0);
//				assertTrue(output == res[i]);
//			agg.close();
//		}	
//	}
//	
//	@Test
//	public void testJoinData(){
//	    // trivial join test
//		
//			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//			 ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//
//			 ch.epfl.dias.ops.volcano.HashJoin join = new HashJoin(scan, scan2, 0, 9);
//			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
//			 agg.open();
//			 DBTuple result = agg.next();
//			 int output = result.getFieldAsInt(0);
//	
//				assertTrue(output == 10);
//			agg.close();
//	}
//	
//	@Test
//	public void testJoinData2(){
//		
//		//  LEFT TABLE HAS ZERO ENTRIES IN JOIN
//
//		 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//		 ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//	
//		 Select sel = new Select(scan, BinaryOp.NE, 0, 10);
//		 
//		 
//		 ch.epfl.dias.ops.volcano.HashJoin join = new HashJoin(sel, scan2, 0, 9);
//		 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);
//		 agg.open();
//		 DBTuple result = agg.next();
//		 int output = result.getFieldAsInt(0);
//	
//			assertTrue(output == 0);
//		agg.close();
//	}
//	
	@Test
	public void testJoinData3(){
		
		// join entries sum
			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
			 ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);

			 ch.epfl.dias.ops.volcano.HashJoin join = new HashJoin(scan, scan2, 0, 9);
			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ProjectAggregate(join, Aggregate.SUM, DataType.INT, 0);
			 agg.open();
			 DBTuple result = agg.next();
			 int output = result.getFieldAsInt(0);
	
				assertTrue(output == 100);
			agg.close();
	}
//	
	@Test
	public void testJoinData4(){
	    // 2 joins cascaded

			 ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
			 ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
			 ch.epfl.dias.ops.volcano.Scan scan3 = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);

			 ch.epfl.dias.ops.volcano.HashJoin join = new HashJoin(scan, scan2, 0, 9);
			 ch.epfl.dias.ops.volcano.HashJoin join2 = new HashJoin(scan3, join, 9, 0);
			 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ProjectAggregate(join2, Aggregate.COUNT, DataType.INT, 0);
			 agg.open();
			 DBTuple result = agg.next();
			 int output = result.getFieldAsInt(0);
			 	assertEquals(100, output);
				//assertTrue(output == 100);
			agg.close();
	}
//	
	
}

