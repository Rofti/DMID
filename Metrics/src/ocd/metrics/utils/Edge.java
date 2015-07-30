package ocd.metrics.utils;

public class Edge {
	double weight;
	Node source;
	Node target;
	
	public Edge(double weight, Node source, Node target) {
		super();
		this.weight = weight;
		this.source = source;
		this.target = target;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public Node getSource() {
		return source;
	}
	public void setSource(Node source) {
		this.source = source;
	}
	public Node getTarget() {
		return target;
	}
	public void setTarget(Node target) {
		this.target = target;
	}
	
}
