package org.apache.giraph.examples.io.formats;


import org.apache.giraph.io.formats.*;
import org.apache.giraph.examples.utils.DMIDVertexValue;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;


import java.io.IOException;
import java.util.Map;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>DMIDVertexValue</code> values and <code>float</code> out-edge weights
 */
public class DMIDVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, DMIDVertexValue,
  DoubleWritable> {

  @Override
  public TextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new DMIDVertexWriter();
  }

 /**
  * VertexWriter that supports vertices with <code>DMIDVertexValue</code>
  * values and <code>float</code> out-edge weights.
  * JSONArray(<vertex id>,
  *   JSONArray(JSONArray(<community id / global leader id>, <membership degree value>), ...))
  * Here is an example with vertex id 1 and two possible communities.
  * First community id 1 contains vertex 1 with a degree of 1/4.
  * Second community id 2 does not contain vertex, the value is 0.0.
  * [1,[[1,1/4],[2,0.0]]]
  */
  private class DMIDVertexWriter extends
    TextVertexWriterToEachLine {
    @Override
    public Text convertVertexToLine(
      Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex
    ) throws IOException {
      JSONArray jsonVertex = new JSONArray();
      try {
        jsonVertex.put(vertex.getId().get());
        
        DMIDVertexValue vertexValue = vertex.getValue();
        JSONArray jsonMemDegArray = new JSONArray();
        for(Map.Entry<Long, Double> entry : vertexValue.getMembershipDegree().entrySet()){
        	if(entry.getValue()!=0){
        		JSONArray jsonDegreeEntry = new JSONArray();
            	jsonDegreeEntry.put(entry.getKey());
            	jsonDegreeEntry.put(entry.getValue());
            	jsonMemDegArray.put(jsonDegreeEntry);
        	}
        	
        }
        jsonVertex.put(jsonMemDegArray);
      } catch (Exception e) {
        throw new IllegalArgumentException(
          "writeVertex: Couldn't write vertex " + vertex);
      }
      return new Text(jsonVertex.toString());
    }
  }
}
