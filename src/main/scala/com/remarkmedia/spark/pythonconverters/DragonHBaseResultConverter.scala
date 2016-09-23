//package com.remarkmedia.spark.pythonconverters
//
//import com.remarkmedia.spark.task.Resource
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import scala.collection.JavaConverters._
//import scala.util.parsing.json.JSONObject
//import org.apache.spark.api.python.Converter
//import org.apache.hadoop.hbase.client.{Put, Result}
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.KeyValue.Type
//import org.apache.hadoop.hbase.CellUtil
//
//
///**
//  * Created by hz on 15/12/30.
//  */
//
//
//class HBaseResultToStringConverter extends Converter[Any, String] {
//  override def convert(obj: Any): String = {
//    val result = obj.asInstanceOf[Result]
//    val output = result.listCells.asScala.map(cell =>
//      Map(
//        "row" -> Bytes.toStringBinary(CellUtil.cloneRow(cell)),
//        "columnFamily" -> Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
//        "qualifier" -> Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
//        "timestamp" -> cell.getTimestamp.toString,
//        "type" -> Type.codeToType(cell.getTypeByte).toString,
//        "value" -> Bytes.toStringBinary(CellUtil.cloneValue(cell))
//      )
//    )
//    output.map(JSONObject(_).toString()).mkString("\n")
//  }
//}
//
//class PostResultToString extends Converter[Any, String] {
//
//
//  override def convert(obj: Any): String = {
//    //    import org.json4s._
//    //    import org.json4s.jackson.JsonMethods._
//    //    implicit val formats = DefaultFormats
//
////    val q = Array[Byte]("q1".toByte)
////    val f = Array[Byte]("value".toByte)
//    val result = obj.asInstanceOf[Result]
//    val value = Bytes.toString(result.getValue("value".getBytes(), "q1".getBytes()))
//    value
//  }
//}
//
//
//class ImmutableBytesWritableToStringConverter extends Converter[Any, String] {
//  override def convert(obj: Any): String = {
//    val key = obj.asInstanceOf[ImmutableBytesWritable]
//    Bytes.toStringBinary(key.get())
//  }
//}
//
////  class ImmutableBytesWritableToByteConverter extends Converter[Any, String] {
////    override def convert(obj: Any):Array[Byte] = {
////      val key = obj.asInstanceOf[ImmutableBytesWritable]
////      key.copyBytes()
////    }
////  }
//
//class StringToImmutableBytesWritableConverter extends Converter[Any, ImmutableBytesWritable] {
//  override def convert(obj: Any): ImmutableBytesWritable = {
//    val bytes = Bytes.toBytes(obj.asInstanceOf[String])
//    new ImmutableBytesWritable(bytes)
//  }
//}
//
//
//class StringListToPutConverter extends Converter[Any, Put] {
//  override def convert(obj: Any): Put = {
//    val output = obj.asInstanceOf[java.util.ArrayList[String]].asScala.map(Bytes.toBytes).toArray
//    val put = new Put(output(0))
//    put.add(output(1), output(2), output(3))
//  }
//}
//
//
//
//
//
