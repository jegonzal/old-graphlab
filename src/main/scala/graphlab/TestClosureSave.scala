package graphlab

import spark._
import org.objectweb.asm._
import javassist._
import java.io._
/**
 * Created with IntelliJ IDEA.
 * User: ylow
 * Date: 11/8/12
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */


object TestClosureSave {
  def main(args: Array[String]) {
    val y = " pikachu"
    val fn = (x:String) => {x + y}

    val csave = new ClosureSave[String,String]

    val bytearray = csave.SaveClosureToMemory(fn)

    var fos = new FileOutputStream("a_closure_of_some_sort")
    fos.write(bytearray)

    // ok now to try to reload it
  }
}
