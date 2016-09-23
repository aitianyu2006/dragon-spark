package com.remarkmedia.spark.task

import com.remarkmedia.spark.common.{HashKey, Hbase, Profile}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by hz on 16/7/12.
  */
object RD_PROFILE extends App {
  val conn = Hbase.createNewConnection()
  HashKey.init()

  if (!conn.getAdmin.tableExists(TableName.valueOf("rd_test"))) {
    val ht = new HTableDescriptor(TableName.valueOf("rd_test"))
    ht.addFamily(new HColumnDescriptor("f1"))
    conn.getAdmin.createTable(ht, HashKey.getSplits())

  }
  conn.close()
}
