package graphlab


import java.lang.reflect.Field
import java.net.MalformedURLException
import java.net.URL
import java.net.URLClassLoader
import java.io._
/**
 * Created with IntelliJ IDEA.
 * User: ylow
 * Date: 11/8/12
 * Time: 9:21 PM
 * To change this template use File | Settings | File Templates.
 */

class ClosureInvoke(wrappedclosure:AnyRef) {
  var runmethod = wrappedclosure
                          .getClass()
                          .getMethod("run", (new Object).getClass())
  def apply(param:AnyRef) = {
    runmethod.invoke(wrappedclosure, param)
  }
}

class ClosureLoad[S, T] {

  def AddToClassPath(u:URL) {
    try {
      val sysLoader = ClassLoader.getSystemClassLoader()
      if (sysLoader.isInstanceOf[URLClassLoader]) {
        val sysLoader2 = sysLoader.asInstanceOf[URLClassLoader]
        val sysLoaderClass = classOf[URLClassLoader]

        // use reflection to invoke the private addURL method
        var method = sysLoaderClass.getDeclaredMethod("addURL", classOf[URL])
        method.setAccessible(true)
        method.invoke(sysLoader, u)
      }
    }
  }

  def AddToClassPath(u:String) {
    AddToClassPath(new File(u).toURI().toURL())
  }

  /**
   * Loads a closure from the provided class name
   * and data file.
   * Returns a ClosureInvoke object with an apply method
   */
  def LoadClosure(class_name:String,
                   data_file:String) = {
    var c = ClassLoader.getSystemClassLoader().loadClass(class_name)
    var fis = new FileInputStream(data_file)
    var oin = new ObjectInputStream(fis)

    var wrappedclosure = c.cast(oin.readObject())
    new ClosureInvoke(wrappedclosure.asInstanceOf[AnyRef])
  }
}
