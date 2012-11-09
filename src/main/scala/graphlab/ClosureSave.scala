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

class ClosureSave[S, T](save_class_path:String, save_data_file:String) {

  /**
   * Saves a  closure to the provided save_class_path
   * and save_data_path.
   *
   * @param fn The closure S=>T to save
   * @return The class name to use when loading
   */
  def SaveClosure(fn:(S=>T)) = {
    ClosureCleaner.clean(fn)

    var cw = new ClassWriter(ClassWriter.COMPUTE_MAXS)
    val pool = ClassPool.getDefault()

    val classname = "graphlab.WrappedClosure"
    val ctc = pool.get(classname)
    val DATE_FORMAT = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS")
    var dstring = DATE_FORMAT.format(new java.util.Date())
    dstring = dstring + scala.util.Random.nextInt(10000).toString()
    ctc.setName(classname + dstring)
    ctc.writeFile(save_class_path)
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


    var fos = new FileOutputStream(save_data_file)
    var out = new ObjectOutputStream(fos)
    out.writeObject(instance)
    fos.close()
    classname + dstring
  }
}
