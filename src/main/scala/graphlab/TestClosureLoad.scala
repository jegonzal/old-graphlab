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


    var fos = new FileInputStream("a_closure_of_some_sort")
    val filelength = new File("a_closure_of_some_sort").length()
    var bytes = new Array[Byte](filelength.toInt)

    val cl = new ClosureLoad()
    fos.read(bytes)
    val closure = cl.LoadClosure(bytes)
    var argument:String = "hello"
    val ret = closure.apply(new String(argument))
    println(ret)

  }
}
