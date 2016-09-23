//package com.remarkmedia.spark.task
//
//import com.remarkmedia.spark.common.{Hbase, HbaseColumn}
//import com.remarkmedia.spark.common.HbaseColumn.Post_mate
//import org.apache.hadoop.hbase.TableName
//import org.apache.hadoop.hbase.client.Scan
//import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
//import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
//import org.apache.hadoop.hbase.util.Bytes
//
///**
//  * Created by hz on 16/6/16.
//  */
//object TestScan extends App with Post_mate {
//  val scan = new Scan()
//  scan.setCacheBlocks(false)
//  scan.setBatch(10000)
//
//  scan.addColumn(HbaseColumn.CM, POST_META_TARGET_ID)
//  scan.addColumn(HbaseColumn.CM, POST_META_ID)
//  scan.addColumn(HbaseColumn.CM, POST_META_TARGET)
//  scan.addColumn(HbaseColumn.CM, POST_META_TIMESTAMP)
//  scan.addColumn(HbaseColumn.CM, POST_META_IDENTITY)
//
//
//  val filter_1 = new SingleColumnValueFilter(HbaseColumn.CM, POST_META_IDENTITY, CompareOp.EQUAL, Bytes.toBytes(2))
//  val filter_2 = new SingleColumnValueFilter(HbaseColumn.CM, POST_META_TARGET, CompareOp.EQUAL, Bytes.toBytes("weibo"))
//
//
//  val filterList = new FilterList()
//  filterList.addFilter(filter_1)
//  filterList.addFilter(filter_2)
//
//  scan.setFilter(filterList)
//
//  val conn = Hbase.createNewConnection()
//  val table = conn.getTable(TableName.valueOf(POST_META_TABLE_NAME))
//
//  val scanner = table.getScanner(scan)
//
//  val it = scanner.iterator()
//
//  while (it.hasNext) {
//    //val post_mate_id = result.getValue(HbaseColumn.CM, POST_META_ID)
//    //val post_mate_targetId = result.getValue(HbaseColumn.CM, POST_META_TARGET_ID)
//    val result = it.next()
//    val target = new String(result.getValue(HbaseColumn.CM, POST_META_TARGET))
//    val post_mate_IDENTITY = Bytes.toInt(result.getValue(HbaseColumn.CM, POST_META_IDENTITY))
//
//    println(target + "  " + post_mate_IDENTITY)
//    //val post_mate_timestamp = result.getValue(HbaseColumn.CM, POST_META_TIMESTAMP)
//  }
//
//
//}
