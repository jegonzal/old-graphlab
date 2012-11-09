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

class MemClassLoader extends ClassLoader {
  def MakeClass(name:String, b:Array[Byte]) = {
    val c = defineClass(name, b, 0, b.length)
    resolveClass(c)
    c
  }
}

class AlternateObjectInputStream(f:InputStream,
                                 loader:MemClassLoader) extends ObjectInputStream(f) {

  override def resolveClass(desc:ObjectStreamClass) = {
    Class.forName(desc.getName(), false, loader)
  }
}

class ClosureLoad[S, T] {

  var OutputClassPath = ""

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

  def SetActiveClassPath(u:String) {
    assert(u.takeRight(1) == "/")
    OutputClassPath = u
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

    val wrappedclosure = c.cast(oin.readObject())
    new ClosureInvoke(wrappedclosure.asInstanceOf[AnyRef])
  }


  /**
   * Loads a closure from the provided class name
   * and data file.
   * Returns a ClosureInvoke object with an apply method
   */
  def LoadClosure(b:Array[Byte]) = {
    var memloader = new MemClassLoader()

    // we have a byte array
    var bis = new ByteArrayInputStream(b)

    // Load the class name
    var ois = new AlternateObjectInputStream(bis, memloader)
    val classname = ois.readObject().asInstanceOf[String]


    var class_byte_code = ois.readObject().asInstanceOf[Array[Byte]]



    var c = memloader.MakeClass(classname, class_byte_code)
    println("Loaded " + c.getName())

    val wrappedclosure = c.cast(ois.readObject())
    new ClosureInvoke(wrappedclosure.asInstanceOf[AnyRef])
  }
}
