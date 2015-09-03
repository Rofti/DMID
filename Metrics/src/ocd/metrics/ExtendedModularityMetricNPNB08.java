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
		/*Node[] nodesA = graph.vertexSet().toArray(
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
		}*/
		int counter=0;
		for(Node nodeA : graph.vertexSet()){
			for(Node nodeB : graph.vertexSet()){
				if(nodeB.getIndex()<=nodeA.getIndex()){
					counter++;
					if(counter %1000000 ==1 ){
					System.out.println("remaining steps " +counter + " / " + (graph.vertexSet().size()*graph.vertexSet().size()+graph.vertexSet().size())/2);
					}
					metricValue += getNodePairModularityContribution(cover, nodeA, nodeB);
				}
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

		/*for (int i = 0; i < cover.communityCount(); i++) {
			comembership += cover.getBelongingFactor(nodeA, i)//made faster, runs only over one nodes communities
					* cover.getBelongingFactor(nodeB, i);
		}*/
		for(Integer comID : nodeA.getOwnCommunities().keySet()){
			if(nodeB.getOwnCommunities().get(comID)!=null){
				comembership += nodeA.getOwnCommunities().get(comID) 
						* nodeB.getOwnCommunities().get(comID) ;
			}
		}

		if (graph.containsEdge(nodeA, nodeB)){
			degProduct = nodeA.getInDegree() * nodeB.getInDegree();//TODO: ursprunglich nur degree aber das gibts nich bei directed
			edgeContribution = (1.0 - (degProduct / (graph.edgeSet().size() * 2)));
			contribution = edgeContribution * comembership;
			
			if(graph.containsEdge(nodeB, nodeA)){
				contribution *= 2;
			}
		}
		else if(graph.containsEdge(nodeB, nodeA)){
			degProduct = nodeA.getInDegree() * nodeB.getInDegree();//TODO
			edgeContribution = (1.0 - (degProduct / (graph.edgeSet().size() * 2)));
			contribution = edgeContribution * comembership;
		}
			
		return contribution;
	}

}
