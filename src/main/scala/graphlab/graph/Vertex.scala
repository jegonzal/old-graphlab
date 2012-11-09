package graphlab.graph

case class Vertex[VertexDataType](id:Int,var data:VertexDataType) {
  var in:Int = 0
  var out:Int = 0

  def num_in_edges:Int = in
  def num_out_edges:Int = out

  def inc_in:Unit = { in += 1 }
  def inc_out:Unit = { out += 1 }
}
