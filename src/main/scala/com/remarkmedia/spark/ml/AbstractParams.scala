package com.remarkmedia.spark.ml

import scala.reflect.runtime.universe._

/**
  * Created by hz on 16/8/29.
  */
abstract class AbstractParams[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  //ConcurrentLinkedQueue

  /**
    * Finds all case class fields in concrete class instance, and outputs them in JSON-style format:
    * {
    * [field name]:\t[field value]\n
    * [field name]:\t[field value]\n
    * ...
    * }
    */
  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"  $paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }
}
