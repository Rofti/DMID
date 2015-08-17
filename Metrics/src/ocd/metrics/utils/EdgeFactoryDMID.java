package ocd.metrics.utils;
import org.jgrapht.EdgeFactory;

public class EdgeFactoryDMID implements EdgeFactory<Node, Edge>{

	@Override
	public Edge createEdge(Node arg0, Node arg1) {
		
		return new Edge(arg0, arg1);
	}

}
