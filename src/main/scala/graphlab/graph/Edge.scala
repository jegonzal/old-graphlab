package graphlab.graph

case class Edge[EdgeDataType,VertexDataType] (id:Int,source:Vertex[VertexDataType],target:Vertex[VertexDataType],var data:EdgeDataType) {
  def get_other_vertex(v:Vertex[VertexDataType]) = {
    if (v == source) {
      target
    } else {
      source
    }
  }
}
