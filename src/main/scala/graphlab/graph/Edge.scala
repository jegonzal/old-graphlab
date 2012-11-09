package graphlab.graph

case class Edge[EdgeDataType,VertexDataType] (id:Int,source:Vertex[VertexDataType],target:Vertex[VertexDataType],var data:EdgeDataType) {
  
}
