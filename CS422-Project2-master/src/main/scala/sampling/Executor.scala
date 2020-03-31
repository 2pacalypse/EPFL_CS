package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object Executor {
  
  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 1)
    // For example, for Q1, params(0) is the interval from the where close
    
    
    
    val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(1)){
     // println("hahaha")
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(1)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    }else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
      select 
      
  l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
	
	from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '%s' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
      """.format(params(0))
    
    
    
    
    
    session.sql(sql).rdd
  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
    
    val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(3))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(3)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
    select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = '%s'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '%s'
	and l_shipdate > date '%s'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
LIMIT 10
      """.format(params(0), params(1), params(1))
    
    
    
    
    session.sql(sql).rdd
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(5))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(5)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
   select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = '%s'
	and o_orderdate >= date '%s'
	and o_orderdate < date '%s' + interval '1' year
group by
	n_name
order by
	revenue desc
      
      """.format(params(0), params(1), params(1))
    
    
    
    
    
    session.sql(sql).rdd
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
       assert(params.size == 3)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(6))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(6)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
  select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '%s'
	and l_shipdate < date '%s' + interval '1' year
	and l_discount between %s - 0.01 and %s + 0.01
	and l_quantity < %s  
      
      """.format(params(0), params(0), params(1), params(1), params(2))
    
   session.sql(sql).rdd
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
           assert(params.size == 2)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(7))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(7)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
 select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			year(l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = '%s' and n2.n_name = '%s')
				or (n1.n_name = '%s' and n2.n_name = '%s')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year
      
      """.format(params(0), params(1), params(1), params(0))
    
    session.sql(sql).rdd
    
    
    
    
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
    
        assert(params.size == 1)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(9))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(9)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			year(o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%%%s%%'
	)  profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
      
      """.format(params(0))
    
    session.sql(sql).rdd
    
    
  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
            assert(params.size == 1)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(10))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(10)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '%s'
	and o_orderdate < date '%s' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
	LIMIT 20
      
      """.format(params(0), params(0))
    
    session.sql(sql).rdd
    
    
    
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
                assert(params.size == 2)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(11))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(11)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = '%s'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * %s
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = '%s'
		)
order by
	value desc
      
      """.format(params(0), params(1), params(0))
    
    session.sql(sql).rdd
    
    
    
    
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
                    assert(params.size == 3)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(12))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(12)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('%s', '%s')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '%s'
	and l_receiptdate < date '%s' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode
      
      """.format(params(0), params(1), params(2), params(2))
    
    session.sql(sql).rdd
    
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
        
                    assert(params.size == 2)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(17))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(17)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = '%s'
	and p_container = '%s'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	)
      
      """.format(params(0), params(1))
    
    session.sql(sql).rdd
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
            
                    assert(params.size == 1)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(18))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(18)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > %s
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
	LIMIT 100
      
      """.format(params(0))
    
    session.sql(sql).rdd
    
    
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
                        assert(params.size == 6)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(19))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(19)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = '%s'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= %s and l_quantity <= %s + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = '%s'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= %s and l_quantity <= %s + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = '%s'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= %s and l_quantity <= %s + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
      
      """.format(params(0), params(3), params(3), params(1), params(4), params(4), params(2), params(5), params(5))
    
    session.sql(sql).rdd
    
    
    
    
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    
                            assert(params.size == 3)
        val sampledesc = desc.sampleDescription.asInstanceOf[List[Int]] 
    if(sampledesc.contains(20))
      session.createDataFrame(
          desc.samples(sampledesc.indexOf(20)).asInstanceOf[RDD[org.apache.spark.sql.Row]],
          desc.lineitem.schema
          ).createOrReplaceTempView("lineitem")
    else
        desc.lineitem.createOrReplaceTempView("lineitem")
    
      
    
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    
    

    
    val sql = """
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like '%s%%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '%s'
					and l_shipdate < date '%s' + interval '1' year
			)
	)
	and s_nationkey = n_nationkey
	and n_name = '%s'
order by
	s_name
      
      """.format(params(0), params(1), params(1), params(2))
    
    session.sql(sql).rdd
    
    
    
    
  }
}
