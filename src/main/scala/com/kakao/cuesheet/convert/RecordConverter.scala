package com.kakao.cuesheet.convert

import java.lang.reflect
import java.lang.reflect.InvocationTargetException

import com.kakao.mango.logging.Logging

import scala.reflect.runtime.universe._


/** an interface to convert any array to type T */
trait RecordConverter[T] extends scala.Predef.Function[Seq[_], T] with Serializable

/** provides RecordMapper[T] instances for Seq, Array and case class types */
object RecordConverter extends Logging {

  val m = runtimeMirror(getClass.getClassLoader)

  def isTuple(clazz: Class[_]) = clazz.getName.startsWith("scala.Tuple")
  def isCaseClass(clazz: Class[_]) = clazz.getInterfaces.contains(classOf[scala.Product])

  def create[T](convert: Seq[_] => T) = new RecordConverter[T] {
    override def apply(seq: Seq[_]): T = convert(seq)
  }
  
  def apply[T: TypeTag]: RecordConverter[T] = {
    val clazz = runtimeMirror(Thread.currentThread().getContextClassLoader).runtimeClass(typeOf[T])

    if (clazz.isArray) {
      val componentType = clazz.getComponentType
      val convert = Converters.converterTo(componentType)
      create { seq =>
        val array = reflect.Array.newInstance(clazz.getComponentType, seq.size)
        for( (element, index) <- seq.zipWithIndex ) {
          reflect.Array.set(array, index, convert(element.asInstanceOf[AnyRef]))
        }
        array.asInstanceOf[T]
      }
    } else if (clazz == classOf[Seq[_]]) {
      create(_.asInstanceOf[T])
    } else {
      val types: Seq[Class[_]] = if (isTuple(clazz)) {
        typeTag[T].tpe.asInstanceOf[TypeRefApi].args.map(m.runtimeClass)
      } else {
        clazz.getConstructors()(0).getParameterTypes
      }
      val converters: Seq[AnyRef => AnyRef] = types.map(Converters.converterTo)
      create(seq => {
        val params = seq.take(converters.size).zipWithIndex.map {
          case (obj: AnyRef, idx) => converters(idx)(obj)
          case _ => null // if not matched to AnyRef, it's null
        }
        try {
          clazz.getConstructors()(0).newInstance(params: _*).asInstanceOf[T]
        } catch {
          case e: InvocationTargetException =>
            logger.error(s"Error while constructing class $clazz", e.getTargetException)
            throw e
          case e: IllegalArgumentException =>
            logger.error(s"Illegal argument provided to constructor of $clazz", e)
            throw e
          case e: Throwable =>
            logger.error(s"Unknown exception while constructing class $clazz", e)
            throw e
        }
      })
    }
  }

}
