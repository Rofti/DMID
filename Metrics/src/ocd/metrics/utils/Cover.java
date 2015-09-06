package ocd.metrics.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.la4j.Matrix;
import org.la4j.matrix.sparse.CCSMatrix;
import org.la4j.Vector;
import org.la4j.Vectors;
import org.jgrapht.graph.*;
/**
 * Represents a cover, i.e. the result of an overlapping community detection algorithm holding the community structure and graph.
 *
 */
public class Cover {

	/**
	 * The graph that the cover is based on.
	 */
	private SimpleDirectedWeightedGraph<Node,Edge> graph = new SimpleDirectedWeightedGraph<Node,Edge>(new EdgeFactoryDMID());	
	/**
	 * The communities forming the cover.
	 */
	//private List<Community> communities = new ArrayList<Community>();
	
	private Matrix memberships= new CCSMatrix();
	/**
	 * Creates a new instance.
	 * @param graph The graph that the cover is based on.
	 */
	public Cover(SimpleDirectedWeightedGraph<Node,Edge> graph) {
		this.graph = graph;
	}
	
	/**
	 * Creates an instance of a cover by deriving the communities from a membership matrix.
	 * Note that the membership matrix (and consequently the cover) will automatically be row-wise normalized according to the 1-norm.
	 * @param graph The corresponding graph.
	 * @param memberships A membership matrix, with non-negative entries. Contains one row for each node and one column for each community.
	 * Entry (i,j) in row i and column j represents the membership degree / belonging factor of the node with index i
	 * with respect to the community with index j.
	 */
	public Cover( SimpleDirectedWeightedGraph<Node,Edge> graph, Matrix memberships) {
		this.graph = graph;
		setMemberships(memberships);
	}

	
	/**
	 * Setter for the graph that the cover is based on.
	 * @param graph The graph.
	 */
	public void setGraph( SimpleDirectedWeightedGraph<Node,Edge> graph) {
		this.graph = graph;
	}
	
	/**
	 * Getter for the graph that the cover is based on.
	 * @return The graph.
	 */
	public SimpleDirectedWeightedGraph<Node,Edge> getGraph() {
		return graph;
	}

	/**
	 * Getter for the membership matrix representing the community structure.
	 * @return The membership matrix. Contains one row for each node and one column for each community.
	 * Entry (i,j) in row i and column j represents the membership degree / belonging factor of the node with index i
	 * with respect to the community with index j. All entries are non-negative and the matrix is row-wise normalized according to the 1-norm.
	 */
	public Matrix getMemberships() {
		/*Matrix memberships = new CCSMatrix(graph.vertexSet().size(), communities.size());
		for(int i=0; i<communities.size(); i++) {
			Community community = communities.get(i);
			for(Map.Entry<Node, Double> membership : community.getMemberships().entrySet()) {
				memberships.set(membership.getKey().getIndex(), i, membership.getValue());
			}
		}*/
		return this.memberships;
	}
	
	/**
	 * Sets the communities from a membership matrix. All metric logs (besides optionally the execution time) will be removed from the cover.
	 * Note that the membership matrix (and consequently the cover) will automatically be row normalized.
	 * @param memberships A membership matrix, with non negative entries. Each row i contains the belonging factors of the node with index i
	 * of the corresponding graph. Hence the number of rows corresponds the number of graph nodes and the number of columns the
	 * number of communities.
	 * @param keepExecutionTime Decides whether the (first) execution time metric log is kept.
	 */
	public void setMemberships(Matrix memberships) {
		if(memberships.rows() != graph.vertexSet().size()) {
			throw new IllegalArgumentException("The row number of the membership matrix must correspond to the graph node count.");
		}
		//communities.clear();
		//memberships = this.normalizeMembershipMatrix(memberships);

		/*for(int j=0; j<memberships.columns(); j++) {
			Community community = new Community();
			communities.add(community);
		}

		for(Node node : graph.vertexSet()){
			NonZeroEntriesVectorProcedure procedure = new NonZeroEntriesVectorProcedure();
			memberships.getRow(node.getIndex()).eachNonZero(procedure);
			List<Integer> nonZeroEntries = procedure.getNonZeroEntries();
			for(int j : nonZeroEntries) {
				Community community = communities.get(j);
				community.setBelongingFactor(node, memberships.get(node.getIndex(), j));
			}
		}*/
		this.memberships=memberships;
	}

	/**
	 * Returns the community count of the cover.
	 * @return The community count.
	 */
	public int communityCount() {
		return memberships.columns();//communities.size();
	}
	
	/**
	 * Returns the indices of the communities that a node is member of.
	 * @param node The node.
	 * @return The community indices.
	 */
	public List<Integer> getCommunityIndices(Node node) {
		List<Integer> communityIndices = new ArrayList<Integer>();
		for(int j=0; j < memberships.columns()/* communities.size()*/; j++) {
			if(memberships.get(node.getIndex(), j)/*this.communities.get(j).getBelongingFactor(node) */> 0) {
				communityIndices.add(j);
			}
		}
		return communityIndices;
	}
	
	/**
	 * Getter for the belonging factor / membership degree of a node for a certain community.
	 * @param node The node.
	 * @param communityIndex The community index.
	 * @return The belonging factor.
	 */
	public double getBelongingFactor(Node node, int communityIndex) {
		return memberships.get(node.getIndex(), communityIndex);
	}
	

	/**
	 * Normalizes each row of a matrix using the one norm.
	 * Note that a unit vector column is added for each row that is equal
	 * to zero to create a separate node community.
	 * @param matrix The memberships matrix to be normalized and set.
	 * @return The normalized membership matrix.
	 */
	public void normalizeMembershipMatrix(Matrix matrix) {
		List<Integer> zeroRowIndices = new ArrayList<Integer>();
		for(int i=0; i<matrix.rows(); i++) {
			Vector row = matrix.getRow(i);
			double norm = row.fold(Vectors.mkManhattanNormAccumulator());
			if(norm != 0) {
				row = row.divide(norm);
				matrix.setRow(i, row);
			}
			else {
				zeroRowIndices.add(i);
			}
		}
		/*
		 * Resizing also rows is required in case there are zero columns.
		 */
		matrix = matrix.copyOfShape(graph.vertexSet().size(), matrix.columns() + zeroRowIndices.size());

		Node curNode;
		Set<Edge> outEdges;
		Set<Edge> inEdges;
		for(int i = 0; i < zeroRowIndices.size(); i++) {
			matrix.set(zeroRowIndices.get(i), matrix.columns() - zeroRowIndices.size() + i, 1d);
			
			
			curNode=new Node(zeroRowIndices.get(i));
			curNode.setInDegree(graph.inDegreeOf(curNode));
			curNode.setOutDegree(graph.outDegreeOf(curNode));
			curNode.addCommunity(matrix.columns() - zeroRowIndices.size() + i, 1d);
			
			outEdges=graph.outgoingEdgesOf(curNode);
			inEdges=graph.incomingEdgesOf(curNode);
			graph.removeVertex(curNode);
			graph.addVertex(curNode);
			
			for(Edge edge:outEdges){
				graph.addEdge(edge.getSource(), edge.getTarget());
			}
			for(Edge edge:inEdges){
				graph.addEdge(edge.getSource(), edge.getTarget());
			}
		}
		this.setMemberships(matrix);
	}
	
	/**
	 * Filters the cover membership matrix by removing insignificant membership values.
	 * The cover is then normalized and empty communities are removed. All metric results
	 * besides the execution time are removed as well.
	 * @param threshold A threshold value, all entries below the threshold will be set to 0, unless they are the maximum 
	 * belonging factor of the node.
	 */
	public void filterMembershipsbyThreshold(double threshold) {
		Matrix memberships = this.getMemberships();
		for(int i=0; i<memberships.rows(); i++) {
			setRowEntriesBelowThresholdToZero(memberships, i, threshold);
		}
		this.setMemberships(memberships);
		removeEmptyCommunities();
	}
	
	/**
	 * Returns the size (i.e. the amount of members) of a certain community.
	 * @param communityIndex The community index.
	 * @return The size.
	 */
	public int getCommunitySize(int communityIndex) {
		NonZeroEntriesVectorProcedure procedure = new NonZeroEntriesVectorProcedure();
		memberships.getColumn(communityIndex).each(procedure);
		List<Integer> nonZeroEntries = procedure.getNonZeroEntries();
		
		return nonZeroEntries.size();//communities.get(communityIndex).getSize();
	}
	
	/**
	 * Filters a matrix row by setting all entries which are lower than a threshold value and the row's max entry to zero.
	 * @param matrix The matrix.
	 * @param rowIndex The index of the row to filter.
	 * @param threshold The threshold.
	 */
	protected void setRowEntriesBelowThresholdToZero(Matrix matrix, int rowIndex, double threshold) {
		Vector row = matrix.getRow(rowIndex);
		double rowThreshold = Math.min(row.fold(Vectors.mkMaxAccumulator()), threshold);
		BelowThresholdEntriesVectorProcedure procedure = new BelowThresholdEntriesVectorProcedure(rowThreshold);
		row.each(procedure);
		List<Integer> belowThresholdEntries = procedure.getBelowThresholdEntries();
		for(int i : belowThresholdEntries) {
			row.set(i, 0);
		}
		matrix.setRow(rowIndex, row);
	}
	
	/**
	 * Removes all empty communities from the graph.
	 * A community is considered to be empty when it does not have any members,
	 * i.e. the corresponding belonging factor equals 0 for each node.
	 */
	protected void removeEmptyCommunities() {

		for(int i=0; i<memberships.columns(); i++) {
			
			if(memberships.getColumn(i).infinityNorm()==0){
				memberships.setColumn(i, memberships.getColumn(memberships.columns()-1));	
				
				memberships=memberships.copyOfShape(memberships.rows(), memberships.columns()-1);
			}
		}
	}
	
}
