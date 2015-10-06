package ocd.algorithm.RAWLPA;

import ocd.metrics.utils.Edge;
import ocd.metrics.utils.Node;
import ocd.metrics.utils.Cover;

import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.la4j.Matrix;
import org.la4j.matrix.dense.Basic2DMatrix;
import org.la4j.matrix.sparse.CCSMatrix;
import org.la4j.Vector;
import org.la4j.Vectors;
import org.la4j.vector.dense.BasicVector;

/**
 * Implements a custom extended version of the Random Walk Label Propagation Algorithm.
 * Handles directed and weighted graphs.
 * For unweighted, undirected graphs, it behaves the same as the original.
 */
public class RandomWalkLabelPropagationAlgorithm{

	/**
	 * The iteration bound for the random walk phase. The default
	 * value is 1000. Must be greater than 0.
	 */
	private int randomWalkIterationBound = 100;

	/**
	 * The profitability step size for the label propagation phase. The default
	 * value is 0.1.  Must be in (0, 1).
	 */
	private double profitabilityDelta = 0.1;
	
	/*
	 * PARAMETER NAMES
	 */
	
	protected static final String PROFITABILITY_DELTA_NAME = "profitabilityDelta";
			
	protected static final String LEADERSHIP_PRECISION_FACTOR_NAME = "leadershipPrecisionFactor";
	
	protected static final String LEADERSHIP_ITERATION_BOUND_NAME = "leadershipIterationBound";

	/**
	 * Creates a standard instance of the algorithm. All attributes are assigned
	 * there default values.
	 */
	public RandomWalkLabelPropagationAlgorithm() {
	}


	public Cover detectOverlappingCommunities(SimpleDirectedWeightedGraph<Node,Edge>  graph){
		System.out.println("Start random-walk phase");
		List<Node> leaders = randomWalkPhase(graph);
		System.out.println("finished random-walk phase\nStart label-propagation phase");
		return labelPropagationPhase(graph, leaders);
	}

	/*
	 * Executes the random walk phase of the algorithm and returns global
	 * leaders.
	 * 
	 * @param graph The graph whose leaders will be detected.
	 * 
	 * @return A list containing all nodes which are global leaders.
	 */
	protected List<Node> randomWalkPhase(SimpleDirectedWeightedGraph<Node,Edge>  graph){
		Matrix disassortativityMatrix = getTransposedDisassortativityMatrix(graph);
		Vector disassortativityVector = executeRandomWalk(disassortativityMatrix);
		Vector leadershipVector = getLeadershipValues(graph,
				disassortativityVector);
		Map<Node, Double> followerMap = getFollowerDegrees(graph,
				leadershipVector);
		return getGlobalLeaders(followerMap);
	}

	/*
	 * Returns the transposed normalized disassortativity matrix for the random
	 * walk phase.
	 * 
	 * @param graph The graph whose disassortativity matrix will be derived.
	 * 
	 * @return The transposed normalized disassortativity matrix.
	 */
	protected Matrix getTransposedDisassortativityMatrix(SimpleDirectedWeightedGraph<Node,Edge>  graph) {
		/*
		 * Calculates transposed disassortativity matrix in a special sparse
		 * matrix format.
		 */
		Matrix disassortativities = new CCSMatrix(graph.vertexSet().size(),
				graph.vertexSet().size());
		Set<Edge> edges = graph.edgeSet();
		double disassortativity;
		
		for(Edge edge : edges){
			disassortativity = Math
					.abs(graph.inDegreeOf(edge.getTarget())
							- graph.inDegreeOf(edge.getSource()));//TODO:was weighted indegree before
			disassortativities.set(edge.getTarget().getIndex(),
					edge.getSource().getIndex(), disassortativity);
		}
		/*
		 * Column normalizes transposed disassortativity matrix.
		 */
		double norm;
		Vector column;
		for (int i = 0; i < disassortativities.columns(); i++) {
			column = disassortativities.getColumn(i);
			norm = column.fold(Vectors.mkManhattanNormAccumulator());
			if (norm > 0) {
				disassortativities.setColumn(i, column.divide(norm));
			}
		}
		return disassortativities;
	}

	/*
	 * Executes the random walk for the random walk phase. The vector is
	 * initialized with a uniform distribution.
	 * 
	 * @param disassortativityMatrix The disassortativity matrix on which the
	 * random walk will be performed.
	 * 
	 * @return The resulting disassortativity vector.
	 */
	protected Vector executeRandomWalk(Matrix disassortativityMatrix){
		Vector vec1 = new BasicVector(disassortativityMatrix.columns());
		for (int i = 0; i < vec1.length(); i++) {
			vec1.set(i, 1.0 / vec1.length());
		}
		//Vector vec2 = new BasicVector(vec1.length());
		int iteration;
		
		for (iteration = 0; /*vec1.subtract(vec2).fold(
				Vectors.mkInfinityNormAccumulator()) > randomWalkPrecisionFactor
				&& */iteration < randomWalkIterationBound; iteration++) {
			if(iteration%10 ==0)
			System.out.println("iteration: "+iteration+"/"+ randomWalkIterationBound);
			//vec2 = vec1.copy();
			vec1 = disassortativityMatrix.multiply(vec1);
		}
		if (iteration > randomWalkIterationBound) {
			System.out.println("Random walk iteration bound exceeded: iteration "
							+ iteration);
			System.exit(0);
		}
		return vec1;
	}

	/*
	 * Calculates the leadership values of all nodes for the random walk phase.
	 * 
	 * @param graph The graph containing the nodes.
	 * 
	 * @param disassortativityVector The disassortativity vector calculated
	 * earlier in the random walk phase.
	 * 
	 * @return A vector containing the leadership value of each node in the
	 * entry given by the node index.
	 */
	protected Vector getLeadershipValues(SimpleDirectedWeightedGraph<Node,Edge>  graph,
			Vector disassortativityVector) {
		Vector leadershipVector = new BasicVector(graph.vertexSet().size());
		Set<Node> nodes = graph.vertexSet();
		double leadershipValue;
		for(Node node :nodes) {
			/*
			 * Note: degree normalization is left out since it
			 * does not influence the outcome.
			 */
			leadershipValue = graph.inDegreeOf(node)
					* disassortativityVector.get(node.getIndex());//TODO: was weightedInDegree before
			leadershipVector.set(node.getIndex(), leadershipValue);
		}
		return leadershipVector;
	}

	/*
	 * Returns the follower degree of each node for the random walk phase.
	 * 
	 * @param graph The graph containing the nodes.
	 * 
	 * @param leadershipVector The leadership vector previous calculated during
	 * the random walk phase.
	 * 
	 * @return A mapping from the nodes to the corresponding follower degrees.
	 */
	protected Map<Node, Double> getFollowerDegrees(SimpleDirectedWeightedGraph<Node,Edge>  graph,
			Vector leadershipVector){
		Map<Node, Double> followerMap = new HashMap<Node, Double>();
		Set<Node> nodes = graph.vertexSet();
		/*
		 * Iterates over all nodes to detect their local leader
		 */
		Set<Edge> edgesFromNode;
		double maxInfluence;
		List<Node> leaders = new ArrayList<Node>();
		Node successor;
		Edge successorEdge;
		double successorInfluence;
		Edge nodeEdge;
		double followerDegree;
		for(Node node : nodes){
			
			edgesFromNode = graph.outgoingEdgesOf(node);
			maxInfluence = Double.NEGATIVE_INFINITY;
			leaders.clear();
			/*
			 * Checks all successors for possible leader
			 */
			for(Edge edge : edgesFromNode){
				successor = edge.getTarget();
				successorEdge = edge;
				successorInfluence = leadershipVector.get(successor.getIndex())
						* graph.getEdgeWeight(successorEdge);
				if (successorInfluence >= maxInfluence) {
					nodeEdge = graph.getEdge(successor, node);
					/*
					 * Ensures the node itself is not a leader of the successor
					 */
					if (nodeEdge == null
							|| successorInfluence > leadershipVector.get(node
									.getIndex()) * graph.getEdgeWeight(nodeEdge)) {
						if (successorInfluence > maxInfluence) {
							/*
							 * Other nodes have lower influence
							 */
							leaders.clear();
						}
						leaders.add(successor);
						maxInfluence = successorInfluence;
					}
				}
			}
			if (!leaders.isEmpty()) {
				for (Node leader : leaders) {
					followerDegree = 0;
					if (followerMap.containsKey(leader)) {
						followerDegree = followerMap.get(leader);
					}
					followerMap.put(leader,
							followerDegree += 1d / leaders.size());
				}
			}
		}
		return followerMap;
	}

	/*
	 * Returns a list of global leaders for the random walk phase.
	 * 
	 * @param followerMap The mapping from nodes to their follower degrees
	 * previously calculated in the random walk phase.
	 * 
	 * @return A list containing all nodes which are considered to be global
	 * leaders.
	 */
	protected List<Node> getGlobalLeaders(Map<Node, Double> followerMap) {
		double averageFollowerDegree = 0;
		for (Double followerDegree : followerMap.values()) {
			
			averageFollowerDegree += followerDegree;
		}
		averageFollowerDegree /= followerMap.size();
		List<Node> globalLeaders = new ArrayList<Node>();
		for (Map.Entry<Node, Double> entry : followerMap.entrySet()) {

			if (entry.getValue() >= averageFollowerDegree) {
				globalLeaders.add(entry.getKey());
			}
		}
		return globalLeaders;
	}

	/*
	 * Executes the label propagation phase.
	 * 
	 * @param graph The graph which is being analyzed.
	 * 
	 * @param leaders The list of global leader nodes detected during the random
	 * walk phase.
	 * 
	 * @return A cover containing the detected communities.
	 */
	protected Cover labelPropagationPhase(SimpleDirectedWeightedGraph<Node,Edge>  graph, List<Node> leaders){
		/*
		 * Executes the label propagation until all nodes are assigned to at
		 * least one community
		 */
		int iterationCount = 0;
		Map<Node, Map<Node, Integer>> communities = new HashMap<Node, Map<Node, Integer>>();
		Map<Node, Integer> communityMemberships;
		do {
			communities.clear();
			iterationCount++;
			for (Node leader : leaders) {
				communityMemberships = executeLabelPropagation(graph, leader, 1
						- iterationCount * profitabilityDelta);
				communities.put(leader, communityMemberships);
			}
			System.out.println("proftiability: "+(1.0-iterationCount * profitabilityDelta) +" all nodes assigned: "+areAllNodesAssigned(graph, communities));
		} while (1 - iterationCount * profitabilityDelta > 0
				&& !areAllNodesAssigned(graph, communities));
		return getMembershipDegrees(graph, communities);
	}

	/*
	 * Executes the label propagation for a single leader to identify its
	 * community members.
	 * 
	 * @param graph The graph which is being analyzed.
	 * 
	 * @param leader The leader node whose community members will be identified.
	 * 
	 * @param profitabilityThreshold The threshold value that determines whether
	 * it is profitable for a node to join the community of the leader / assume
	 * its behavior.
	 * 
	 * @return A mapping containing the iteration count for each node that is a
	 * community member. The iteration count indicates, in which iteration the
	 * corresponding node has joint the community.
	 */
	protected Map<Node, Integer> executeLabelPropagation(SimpleDirectedWeightedGraph<Node,Edge>  graph,
			Node leader, double profitabilityThreshold) {
		Map<Node, Integer> memberships = new HashMap<Node, Integer>();
		int previousMemberCount;
		int iterationCount = 0;
		/*
		 * Iterates as long as new members assume the behavior.
		 */
		Set<Node> predecessors;
		Iterator<Node> nodeIt;
		Node node;
		double profitability;
		Set<Edge> edgesFromNode;
		Node nodeSuccessor;
		do {
			iterationCount++;
			previousMemberCount = memberships.size();
			predecessors = getBehaviorPredecessors(graph, memberships, leader);
			nodeIt = predecessors.iterator();
			/*
			 * Checks for each predecessor of the leader behavior nodes whether
			 * it assumes the new behavior.
			 */
			while (nodeIt.hasNext()) {
				node = nodeIt.next();
				profitability = 0;
				edgesFromNode = graph.outgoingEdgesOf(node);
				for(Edge edge : edgesFromNode){
					nodeSuccessor = edge.getTarget();
					Integer joinIteration = memberships.get(nodeSuccessor);
					if (nodeSuccessor.equals(leader) || 
							( joinIteration != null && joinIteration < iterationCount)) {
						profitability++;
					}
				}
				if (profitability / (double) edgesFromNode.size() > profitabilityThreshold) {
					memberships.put(node, iterationCount);
				}
			}
		} while (memberships.size() > previousMemberCount);
		return memberships;
	}

	/*
	 * Returns all predecessors of the nodes which adopted the leader's behavior
	 * (and the leader itself) for the label propagation of each leader.
	 * 
	 * @param graph The graph which is being analyzed.
	 * 
	 * @param memberships The nodes which have adopted leader behavior. Note
	 * that the membership degrees are not examined, any key value is considered
	 * a node with leader behavior.
	 * 
	 * @param leader The node which is leader of the community currently under
	 * examination.
	 * 
	 * @return A set containing all nodes that have not yet assumed leader
	 * behavior, but are predecessors of a node with leader behavior.
	 */
	protected Set<Node> getBehaviorPredecessors(SimpleDirectedWeightedGraph<Node,Edge>  graph,
			Map<Node, Integer> memberships, Node leader) {
		Set<Node> neighbors = new HashSet<Node>();
		Set<Edge> edgesToleader = graph.incomingEdgesOf(leader);
		Node leaderPredecessor;
		for(Edge edge : edgesToleader) {
			leaderPredecessor = edge.getSource();
			if (!memberships.containsKey(leaderPredecessor)) {
				neighbors.add(leaderPredecessor);
			}
		}
		Set<Edge> edgesToMember;
		Node memberPredecessor;
		for (Node member : memberships.keySet()) {

			edgesToMember = graph.incomingEdgesOf(member);
			for(Edge edge : edgesToMember){
				memberPredecessor = edge.getSource();
				if (!memberPredecessor.equals(leader)
						&& !memberships.containsKey(memberPredecessor)) {
					neighbors.add(memberPredecessor);
				}
			}
		}
		return neighbors;
	}

	/*
	 * Indicates for the label propagation phase whether all nodes have been
	 * assigned to at least one community.
	 * 
	 * @param graph The graph which is being analyzed.
	 * 
	 * @param communities A mapping from the leader nodes to the membership
	 * degrees of that leaders community.
	 * 
	 * @return TRUE when each node has been assigned to at least one community,
	 * and FALSE otherwise.
	 */
	protected boolean areAllNodesAssigned(SimpleDirectedWeightedGraph<Node,Edge>  graph,
			Map<Node, Map<Node, Integer>> communities){
		boolean allNodesAreAssigned = true;
		Set<Node> nodes = graph.vertexSet();
		boolean nodeIsAssigned;

		for(Node node : nodes){

			nodeIsAssigned = false;
			for (Map.Entry<Node, Map<Node, Integer>> entry : communities
					.entrySet()) {
				if (entry.getValue().containsKey(node)) {
					nodeIsAssigned = true;
					break;
				}
			}
			if (!nodeIsAssigned) {
				allNodesAreAssigned = false;
				break;
			}
		}
		return allNodesAreAssigned;
	}

	/*
	 * Returns a cover containing the membership degrees of all nodes.,
	 * calculated from
	 * 
	 * @param graph The graph which is being analyzed.
	 * 
	 * @param communities A mapping from the leader nodes to the iteration count
	 * mapping of their community members.
	 * 
	 * @return A cover containing each nodes membership degree
	 */
	protected Cover getMembershipDegrees(SimpleDirectedWeightedGraph<Node,Edge> graph,
			Map<Node, Map<Node, Integer>> communities){
		Matrix membershipMatrix = new Basic2DMatrix(graph.vertexSet().size(),
				communities.size());
		int communityIndex = 0;
		double membershipDegree;
		for (Node leader : communities.keySet()) {
			membershipMatrix.set(leader.getIndex(), communityIndex, 1.0);
			for (Map.Entry<Node, Integer> entry : communities.get(leader)
					.entrySet()) {
				membershipDegree = 1.0 / Math.pow(entry.getValue(), 2);
				//TODO: added a delete Memdegrees below certain value to get less data maybe stupid reconsider
				//if(membershipDegree>0.3){
				membershipMatrix.set(entry.getKey().getIndex(), communityIndex,
						membershipDegree);
				//}
			}
			communityIndex++;
		}
		Cover cover = new Cover(graph, membershipMatrix);
		return cover;
	}
}