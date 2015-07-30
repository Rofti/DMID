package ocd.metrics;

import ocd.metrics.utils.Cover;
import ocd.metrics.utils.Edge;
import ocd.metrics.utils.Node;

import org.la4j.vector.Vectors;
import org.jgrapht.graph.*;

/**
 * Implements the extended modularity metric.
 */
public class ExtendedModularityMetric {
		
	public ExtendedModularityMetric() {
	}

	public double measure(Cover cover) {
		double metricValue = 0;
		SimpleDirectedWeightedGraph<Node,Edge> graph = cover.getGraph();
		Node[] nodesA = graph.vertexSet().toArray(new Node[graph.vertexSet().size()]);
		Node[] nodesB = graph.vertexSet().toArray(new Node[graph.vertexSet().size()]);
		Node nodeA;
		Node nodeB;
		int j=0;
		for(int i=0; i< nodesA.length;++i){
			nodeA = nodesA[i];
			j=0;
			while(j<=i) {
				nodeB = nodesB[j];
				metricValue +=
						getNodePairModularityContribution(cover, nodeA, nodeB);
				j++;
			}
		}
		if(graph.edgeSet().size() > 0) {
			metricValue /= graph.edgeSet().size();
		}
		return metricValue;
	}
	
	/**
	 * Returns the belonging coefficient of an edge for a certain community.
	 * @param cover The cover being measured.
	 * @param sourceNode The source node of the edge.
	 * @param targetNode The target node of the edge.
	 * @param communityIndex The community index.
	 * @return The belonging coefficient.
	 */
	private double getEdgeBelongingCoefficient(Cover cover, Node sourceNode, Node targetNode, int communityIndex) {
		return cover.getBelongingFactor(sourceNode, communityIndex) * cover.getBelongingFactor(targetNode, communityIndex);
	}
	
	/**
	 * Returns the modularity index contribution by the null model for two given nodes a and b and a certain community.
	 * This contains the contribution for edge a -> b and edge b -> a.
	 * @param cover The cover being measured.
	 * @param nodeA The first node.
	 * @param nodeB The second node.
	 * @param communityIndex The community index.
	 * @return The null model contribution value.
	 */
	private double getNullModelContribution(Cover cover, Node nodeA, Node nodeB, int communityIndex) {
		double coeff = cover.getBelongingFactor(nodeA, communityIndex);
		coeff *= cover.getBelongingFactor(nodeB, communityIndex);
		SimpleDirectedWeightedGraph<Node, Edge> graph = cover.getGraph();
		if(nodeA.getIndex() != nodeB.getIndex()) {
			coeff *= graph.outDegreeOf(nodeA) * graph.inDegreeOf(nodeB) + graph.inDegreeOf(nodeA) * graph.outDegreeOf(nodeB);
		}
		else {
			coeff *= graph.outDegreeOf(nodeA) * graph.inDegreeOf(nodeB);
		}
		if(coeff != 0) {
			coeff /= Math.pow(graph.vertexSet().size(), 2);
			/*
			 * Edge count cannot be 0 here due to the node degrees.
			 */
			coeff /= graph.edgeSet().size();
			coeff *= Math.pow(cover.getMemberships().getColumn(communityIndex).fold(Vectors.mkManhattanNormAccumulator()), 2);
		}
		return coeff;
	}

	/**
	 * Returns the modularity index for the two nodes a and b.
	 * This includes the edges a -> b and b -> a.
	 * @param cover The cover being measured.
	 * @param nodeA The first node.
	 * @param nodeB The second node.
	 * @return The modularity index for nodes a and b with regard to all communities.
	 */
	private double getNodePairModularityContribution(Cover cover, Node nodeA, Node nodeB) {
		double edgeContribution = 0;
		for(int i=0; i<cover.communityCount(); i++) {
			double coverContribution = 0;
			SimpleDirectedWeightedGraph<Node, Edge> graph = cover.getGraph();
			if(graph.containsEdge(nodeA, nodeB)) {
				coverContribution += getEdgeBelongingCoefficient(cover, nodeA, nodeB, i);
			}
			if(graph.containsEdge(nodeB,nodeA)) {
				coverContribution += getEdgeBelongingCoefficient(cover, nodeB, nodeA, i);
			}
			double nullModelContribution = getNullModelContribution(cover, nodeA, nodeB, i);
			edgeContribution += coverContribution - nullModelContribution;
		}
		return edgeContribution;
	}

}
