package ocd.metrics;

import ocd.metrics.utils.Cover;
import ocd.metrics.utils.Edge;
import ocd.metrics.utils.Node;

import org.jgrapht.graph.SimpleDirectedWeightedGraph;

public class ExtendedModularityMetricNPNB08 {

	public ExtendedModularityMetricNPNB08() {
	}

	public double measure(Cover cover) {
		double metricValue = 0;
		SimpleDirectedWeightedGraph<Node, Edge> graph = cover.getGraph();
		Node[] nodesA = graph.vertexSet().toArray(
				new Node[graph.vertexSet().size()]);
		Node[] nodesB = graph.vertexSet().toArray(
				new Node[graph.vertexSet().size()]);
		Node nodeA;
		Node nodeB;
		int j = 0;
		for (int i = 0; i < nodesA.length; ++i) {
			nodeA = nodesA[i];
			j = 0;
			while (j <= i) {
				nodeB = nodesB[j];
				metricValue += getNodePairModularityContribution(cover, nodeA,
						nodeB);
				j++;
			}
		}
		if (graph.edgeSet().size() > 0) {
			metricValue /= graph.edgeSet().size();
		}
		return metricValue;
	}

	private double getNodePairModularityContribution(Cover cover, Node nodeA,
			Node nodeB) {

		double contribution = 0;
		double edgeContribution = 0;
		SimpleDirectedWeightedGraph<Node, Edge> graph = cover.getGraph();
		double degProduct;
		double comembership = 0;

		for (int i = 0; i < cover.communityCount(); i++) {
			comembership += cover.getBelongingFactor(nodeA, i)
					* cover.getBelongingFactor(nodeB, i);
		}

		if (graph.containsEdge(nodeA, nodeB)
				|| graph.containsEdge(nodeB, nodeA)) {

			degProduct = graph.degreeOf(nodeA) * graph.degreeOf(nodeB);
			edgeContribution = (1.0 - (degProduct / (graph.edgeSet().size() * 2)));
			contribution = edgeContribution * comembership;

			if (graph.containsEdge(nodeA, nodeB)
					&& graph.containsEdge(nodeB, nodeA)) {
				contribution *= 2;
			}
		}

		return contribution;
	}

}
