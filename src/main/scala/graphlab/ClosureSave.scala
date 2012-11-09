package graphlab
import spark._
import org.objectweb.asm._
import javassist._
import java.io._

/**
 * Created with IntelliJ IDEA.
 * User: ylow
 * Date: 11/8/12
 * Time: 9:00 PM
 * To change this template use File | Settings | File Templates.
 */

class WrappedClosure[S,T] extends java.io.Serializable {
  var fn:(S => T) = null
  def run(x:S):T = {
    return fn(x)
  }
}

class ClosureSave[S, T] {

  /**
   * Returns of tuple of
   * (A CtClass containing the wrangled
   *  class name, the wrangled classname,
   *  and an instance of the wrangled class with
   *  the lambda attached)
   */
  private def CreateObjectWebCtclass(fn:(S=>T)) = {
    ClosureCleaner.clean(fn)
    var cw = new ClassWriter(ClassWriter.COMPUTE_MAXS)
    val pool = ClassPool.getDefault()

    val classname = "graphlab.WrappedClosure"
    val ctc = pool.get(classname)
    val DATE_FORMAT = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS")
    var dstring = DATE_FORMAT.format(new java.util.Date())
    dstring = dstring + scala.util.Random.nextInt(10000).toString()
    ctc.setName(classname + dstring)
    ctc.freeze()

    var instance_class = ctc.toClass()
    var instance = instance_class.newInstance()

    //recursive_print_class(instance_class, instance)
    // use the accessor to set the fn field
    instance_class
      .getMethods
      .find(_.getName ==  "fn_$eq")
      .get
      .invoke(instance, fn.asInstanceOf[AnyRef])

    (ctc, classname + dstring, instance)
  }

  /**
   * Saves a  closure to the provided save_class_path
   * and save_data_path.
   *
   * @param fn The closure S=>T to save
   * @return The class name to use when loading
   */
  def SaveClosure(fn:(S=>T),
                  save_class_path:String,
                  save_data_file:String) = {

    val (ctc, classname, instance) = CreateObjectWebCtclass(fn)

    ctc.writeFile(save_class_path)


    var fos = new FileOutputStream(save_data_file)
    var out = new ObjectOutputStream(fos)
    out.writeObject(instance)
    fos.close()
    classname
  }

  // returns a byte array you can transport
  def SaveClosureToMemory(fn:(S=>T)) = {

    val (ctc, classname, instance) = CreateObjectWebCtclass(fn)

    var bos = new ByteArrayOutputStream()
    var out = new ObjectOutputStream(bos)

    out.writeObject(classname)
    out.flush()

    // ok lets write the class to a new byte array
    val bytecode_output = ctc.toBytecode()

    // write the byte array
    out.writeObject(bytecode_output)
    out.writeObject(instance)
    out.flush()

    // the format is
    // String ==> class name
    // byte Array for the DataOutputStream containing the class file contents
    // ??? ==> The instance object
    bos.toByteArray()
    // now write the jar file
  }
}
