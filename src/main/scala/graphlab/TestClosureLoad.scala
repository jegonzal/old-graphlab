package graphlab

import java.lang.reflect.Field
import java.net.MalformedURLException
import java.net.URL
import java.net.URLClassLoader
import java.io._
;
/**
 * Created with IntelliJ IDEA.
 * User: ylow
 * Date: 11/8/12
 * Time: 3:44 PM
 * To change this template use File | Settings | File Templates.
 */
object TestClosureLoad {

  def main(args: Array[String]) {
    val cl = new ClosureLoad()
    cl.AddToClassPath("classes/")
    val classname = readLine()

    val closure = cl.LoadClosure(classname,"data")
    var argument:String = "hello"
    val ret = closure.apply(new String(argument))
    println(ret)
    //println(wrappedclosure.run("world"))
  }
}
