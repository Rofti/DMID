package org.apache.giraph.examples.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.*;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.utils.DMIDVertexValue;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>DMIDVertexValue</code> vertex values and <code>float</code>
  * out-edge weights, and <code>LongDoubleMessage</code> message types,
  * 
  *  specified in JSON format.
  */
public class DMIDVertexInputFormat extends
TextVertexInputFormat<LongWritable, DMIDVertexValue, DoubleWritable>{

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new DMIDVertexReader();
  }

 /**
  * VertexReader that features <code>DMIDVertexValue</code> vertex
  * values and <code>float</code> out-edge weights. The
  * files should be in the following JSON format:
  * JSONArray(<vertex id>,
  *   JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
  * Here is an example with vertex id 1 and two edges.
  * First edge has a destination vertex 2, edge value 2.1.
  * Second edge has a destination vertex 3, edge value 0.7.
  * [1,[[2,2.1],[3,0.7]]]
  */
  class DMIDVertexReader extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new LongWritable(jsonVertex.getLong(0));
    }

    @Override
    protected DMIDVertexValue getValue(JSONArray jsonVertex) throws
      JSONException, IOException {
      return new DMIDVertexValue();
    }

    @Override
    protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges(
        JSONArray jsonVertex) throws JSONException, IOException {
      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(1);
      List<Edge<LongWritable, DoubleWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());
      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
        edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(0)),
            new DoubleWritable( jsonEdge.getDouble(1))));
      }
      return edges;
    }

    @Override
    protected Vertex<LongWritable, DMIDVertexValue, DoubleWritable>
    handleException(Text line, JSONArray jsonVertex, JSONException e) {
      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }

  }
}
