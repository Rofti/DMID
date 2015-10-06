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

		long counter=0;
		long laststep=((long) graph.vertexSet().size())*((long)graph.vertexSet().size())+graph.vertexSet().size();

		for(Node nodeA : graph.vertexSet()){
			for(Node nodeB : graph.vertexSet()){

					counter++;
					if(counter %10000 ==1 ){
					System.out.println("remaining steps " +counter/10000 + " / " + laststep/10000);
					}
					metricValue += getNodePairModularityContribution(cover, nodeA, nodeB);

			}
		}
		
		if (graph.edgeSet().size() > 0) {
			metricValue /= (graph.edgeSet().size()*2);
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

		for(Integer comID : cover.getCommunityIndices(nodeA)){
			
			if( cover.getCommunityIndices(nodeB).contains(comID)){
				
				comembership += (nodeA.getOwnCommunities().get(comID)
						* nodeB.getOwnCommunities().get(comID)) ;
				
			}
		}
		
		double adjancencyEntry=0;
		if (graph.containsEdge(nodeA, nodeB)){
			adjancencyEntry=1;
		}	

		degProduct = nodeA.getInDegree() * nodeB.getInDegree();//TODO: ursprunglich nur degree aber das gibts nich bei directed
		edgeContribution = (adjancencyEntry- (degProduct / (graph.edgeSet().size() *2 )));
		contribution = edgeContribution * comembership;
			
			
		return contribution;
	}

}
