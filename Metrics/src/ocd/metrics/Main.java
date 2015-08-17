package ocd.metrics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import ocd.metrics.utils.Cover;
import ocd.metrics.utils.Edge;
import ocd.metrics.utils.EdgeFactoryDMID;
import ocd.metrics.utils.Node;

import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.commons.math3.stat.ranking.NaturalRanking;
import org.apache.commons.math3.stat.ranking.RankingAlgorithm;
import org.apache.commons.math3.stat.ranking.TiesStrategy;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.json.JSONArray;
import org.json.JSONException;
import org.la4j.matrix.Matrix;
import org.la4j.matrix.sparse.CCSMatrix;

public class Main {

	public static void main(String[] args) throws NumberFormatException,
			IOException, JSONException {

		SimpleDirectedWeightedGraph<Node, Edge> graph;
		Cover cover;
		FileWriter output;

		/** -mode - */
		switch (Integer.parseInt(args[0])) {
		case 1:
			/**
			 * ExtendedModularity -inputPathGraph -inputPathCommunities
			 * -outputPath
			 */
			if (args.length != 4) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -inputPathCommunities -outputPath");
			}
			graph = readGraph(args[1]);
			cover = readCover(graph, args[2]);
			ExtendedModularityMetric modularityMetric = new ExtendedModularityMetric();
			double modularityValue = modularityMetric.measure(cover);

			output = new FileWriter(args[3], true);
			output.write("\n" + "Graph: " + args[1] + "\tCover: " + args[2]
					+ "\tExtendedModularity= " + modularityValue);
			output.flush();
			output.close();
			break;
		case 2:
			/**
			 * NMI -inputPathGraph -inputPathCommunities -inputPathGroundTruth
			 * -outputPath
			 */
			if (args.length != 5) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -inputPathCommunities -inputPathGroundTruth -outputPath");
			}

			graph = readGraph(args[1]);
			cover = readCover(graph, args[2]);
			Cover groundTruth = readCover(graph, args[3]);

			cover.filterMembershipsbyThreshold(1);
			groundTruth.filterMembershipsbyThreshold(1);

			ExtendedNormalizedMutualInformationMetric nmiMetric = new ExtendedNormalizedMutualInformationMetric();
			double nmiValue = nmiMetric.measure(cover, groundTruth);

			output = new FileWriter(args[4], true);
			output.write("\n" + "Graph: " + args[1] + "\tCover: " + args[2]
					+ "\tgroundTruth:" + args[3] + "\tNMI= " + nmiValue);
			output.flush();
			output.close();

			break;
		case 3:
			/**
			 * ExtendedModularityNPNB08 -inputPathGraph -inputPathCommunities
			 * -outputPath
			 */
			if (args.length != 4) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -inputPathCommunities -outputPath");
			}
			graph = readGraph(args[1]);
			cover = readCover(graph, args[2]);
			ExtendedModularityMetricNPNB08 modularityMetricNPNB08 = new ExtendedModularityMetricNPNB08();
			double modularityNPNB08Value = modularityMetricNPNB08
					.measure(cover);

			output = new FileWriter(args[3], true);
			output.write("\n" + "Graph: " + args[1] + "\tCover: " + args[2]
					+ "\tExtendedModularityNPNB08= " + modularityNPNB08Value);
			output.flush();
			output.close();
			break;
		case 4:
			/**
			 * SpearmanMeasure -inputPathGraph -outputPath
			 */
			if (args.length != 3) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -outputPath");
			}
			graph = readGraph(args[1]);
			spearmanMeasure(graph, args[2]);
			break;
		case 5:
			if (args.length != 3) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -outputPath");
			}

			graph = readGraph(args[1]);
			System.out.println("readGraph finished");
			graph = cleanBrokenIDs(graph);
			System.out.println("cleanBrokenIds finished");
			writeDMIDInputFormat(graph,args[2]);
			
			break;		
		default:
			break;
		}

	}

	public static SimpleDirectedWeightedGraph<Node, Edge> readGraph(
			String inputPath) throws NumberFormatException, IOException {

		BufferedReader br = new BufferedReader(new FileReader(inputPath));
		String line;

		long numNodes = 0;
		long numEdges = 0;
		long srcID;
		long destID;
		long edgeCounter = 0;
		boolean isDirected=true;
		SimpleDirectedWeightedGraph<Node, Edge> graph = new SimpleDirectedWeightedGraph<Node, Edge>(
				new EdgeFactoryDMID());

		while ((line = br.readLine()) != null) {
			if(line.startsWith("# Undirected")){
				System.out.println("is undirected");
				isDirected=false;
			}
			
			if (line.startsWith("# Nodes:")) {

				line = line.substring(9, line.length());
				numNodes = Long.parseLong(line.substring(0, line.indexOf(" ")));

				line = line.substring(line.lastIndexOf(" ") + 1, line.length());
				numEdges = Long.parseLong(line) / 2;

				System.out.println("NumNodes: " + numNodes + "\t numEdges: "
						+ numEdges);

			} else if (!line.startsWith("#")) {

				line = line.replace("\t", " ");
				srcID = Long.parseLong(line.substring(0, line.indexOf(" ")));
				destID = Long.parseLong(line.substring(line.indexOf(" ") + 1,
						line.length()));

				Node srcNode = new Node((int) srcID);
				Node destNode = new Node((int) destID);
				graph.addVertex(srcNode);
				graph.addVertex(destNode);
				
				graph.addEdge(srcNode, destNode);

				if(!isDirected){
					graph.addEdge(destNode, srcNode);
				}
					
				edgeCounter++;
				if (edgeCounter % 100000 == 0) {
					System.out.println("Line " + edgeCounter + ",  edge:"
							+ line);
				}

			}
		}

		br.close();

		return graph;
	}

	public static Cover readCover(
			SimpleDirectedWeightedGraph<Node, Edge> graph,
			String inputPathCommunities) throws NumberFormatException,
			IOException, JSONException {
		Cover cover = new Cover(graph);

		BufferedReader br = new BufferedReader(new FileReader(
				inputPathCommunities));
		String line;

		JSONArray membershipVector;
		JSONArray memDegree;
		Matrix memberships = new CCSMatrix();

		if ((line = br.readLine()) != null) {
			membershipVector = new JSONArray(line);
			memberships = new CCSMatrix(graph.vertexSet().size(),
					membershipVector.length() - 1);

			for (int i = 1; i < membershipVector.length(); ++i) {
				memDegree = membershipVector.getJSONArray(i);
				memberships.set(membershipVector.getInt(0),
						memDegree.getInt(0), memDegree.getDouble(1));
			}
		} else {
			System.out.println("Error: Empty Cover");
		}

		while ((line = br.readLine()) != null) {
			// [1,[[1,1/4],[2,0.0]]]
			membershipVector = new JSONArray(line);
			for (int i = 1; i < membershipVector.length(); ++i) {
				memDegree = membershipVector.getJSONArray(i);
				memberships.set(membershipVector.getInt(0),
						memDegree.getInt(0), memDegree.getDouble(1));
			}
		}

		br.close();
		cover.setMemberships(memberships);

		return cover;
	}

	public static void writeDMIDInputFormat(
			SimpleDirectedWeightedGraph<Node, Edge> graph, String outputPath)
			throws IOException {

		FileWriter output = new FileWriter(outputPath, false);

		Set<Edge> outgoingEdges;
		JSONArray singleNode;
		JSONArray singleOutEdge;
		JSONArray allOutEdges;
		int remainingNodes = graph.vertexSet().size();
		System.out.println("Start writing Output");
		for (Node node : graph.vertexSet()) {

			singleNode = new JSONArray();
			singleNode.put(node.getIndex());

			outgoingEdges = graph.outgoingEdgesOf(node);
			allOutEdges = new JSONArray();

			for (Edge outEdge : outgoingEdges) {
				
				singleOutEdge = new JSONArray();
				singleOutEdge.put(outEdge.getTarget().getIndex());
				singleOutEdge.put(new Double(graph.getEdgeWeight(outEdge)));
				
				allOutEdges.put(singleOutEdge);
			}
			
			singleNode.put(allOutEdges);
			output.write(singleNode.toString());
			
			remainingNodes--;
			if(remainingNodes !=0){
				output.write("\n");
			}
		}
		output.flush();
		output.close();
	}

	public static void spearmanMeasure(
			SimpleDirectedWeightedGraph<Node, Edge> graph, String outputPath)
			throws IOException {

		double[] dataX = new double[graph.edgeSet().size()];
		double[] dataY = new double[graph.edgeSet().size()];

		Edge[] edges = graph.edgeSet()
				.toArray(new Edge[graph.edgeSet().size()]);

		for (int i = 0; i < graph.edgeSet().size(); ++i) {
			if (Math.random() > (1 / 2d)) {
				dataX[i] = graph.degreeOf(edges[i].getSource()) + Math.random();
				dataY[i] = graph.degreeOf(edges[i].getTarget()) + Math.random();
			} else {
				dataX[i] = graph.degreeOf(edges[i].getTarget()) + Math.random();
				dataY[i] = graph.degreeOf(edges[i].getSource()) + Math.random();
			}
		}

		RankingAlgorithm natural = new NaturalRanking(NaNStrategy.MINIMAL,
				TiesStrategy.SEQUENTIAL);
		SpearmansCorrelation spearmansCor = new SpearmansCorrelation(natural);
		double spearmanRank = spearmansCor.correlation(dataX, dataY);

		FileWriter output = new FileWriter(outputPath, true);
		output.write("\n" + outputPath + "\t Spearmans rho= " + spearmanRank
				+ "\t Edges=" + graph.edgeSet().size() + "\t Nodes= "
				+ graph.vertexSet().size());
		output.flush();
		output.close();
	}

	public static SimpleDirectedWeightedGraph<Node, Edge> cleanBrokenIDs(
			SimpleDirectedWeightedGraph<Node, Edge> graph) {
		Set<Node> nodeSet = graph.vertexSet();
		int numNodes = nodeSet.size();

		HashSet<Integer> unusedIDs = new HashSet<Integer>();
		for (int i = 0; i < numNodes; ++i) {
			unusedIDs.add(new Integer(i));
		}
		System.out.println("prepared UnusedIDs");
		System.out.println("NumNodes: "+numNodes);
		HashSet<Node> brokenNodes = new HashSet<Node>();
		for (Node node : nodeSet) {
			if (node.getIndex() < numNodes) {
				
				unusedIDs.remove(new Integer(node.getIndex()));
			} else {
				brokenNodes.add(node);
			}
		}
		System.out.println("identified brokenNodes");
		if (brokenNodes.size() != unusedIDs.size()) {
			System.out
					.println("Error: more nodes over ID limit then unused IDs \n Ids: "+ unusedIDs.size() + " brokenNodes: "+ brokenNodes.size());
			System.exit(0);
		}

		Iterator<Integer> iter;
		Integer id;
		Node fixedNode;
		Set<Edge> outgoingB;
		Set<Edge> incomingB;
		System.out.println("start fixing nodes");
		for (Node nodeB : brokenNodes) {
			
			nodeSet = graph.vertexSet();

			iter = unusedIDs.iterator();
			id = iter.next();

			unusedIDs.remove(id);
			fixedNode = new Node(id);
			graph.addVertex(fixedNode);

			outgoingB = graph.outgoingEdgesOf(nodeB);
			incomingB = graph.incomingEdgesOf(nodeB);

			for (Edge edge : outgoingB) {
				graph.addEdge(fixedNode, edge.getTarget());
			}

			for (Edge edge : incomingB) {
				graph.addEdge(edge.getSource() ,fixedNode);
			}

			graph.removeVertex(nodeB);

		}
		System.out.println("remaining unused ids: "+ unusedIDs.size());
		return graph;
	}

}
