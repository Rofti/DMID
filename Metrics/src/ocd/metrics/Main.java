package ocd.metrics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import ocd.metrics.utils.BelowThresholdEntriesVectorProcedure;
import ocd.metrics.utils.Cover;
import ocd.metrics.utils.Edge;
import ocd.metrics.utils.EdgeFactoryDMID;
import ocd.metrics.utils.Node;
import ocd.algorithm.SLPA.SLPA;

import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.commons.math3.stat.ranking.NaturalRanking;
import org.apache.commons.math3.stat.ranking.RankingAlgorithm;
import org.apache.commons.math3.stat.ranking.TiesStrategy;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.json.JSONArray;
import org.json.JSONException;
import org.la4j.Matrix;
import org.la4j.matrix.sparse.CCSMatrix;
import org.la4j.Vector;
import org.la4j.Vectors;


public class Main {

	public static void main(String[] args) throws NumberFormatException,
			IOException, JSONException {

		SimpleDirectedWeightedGraph<Node, Edge> graph;
		Cover cover;
		FileWriter output;
		BufferedReader br =null;

		/** -mode - */
		switch (Integer.parseInt(args[0])) {
		case 1:
			/**
			 * ExtendedModularity -inputPathGraph(DMIDFormat) -inputPathCommunities
			 * -outputPath
			 */
			if (args.length != 4) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -inputPathCommunities -outputPath");
			}
			cover = readCoverAndGraph(args[1], args[2]);
			System.out.println("Calculate extended modularity");
			ExtendedModularityMetric modularityMetric = new ExtendedModularityMetric();
			double modularityValue = modularityMetric.measure(cover);

			System.out.println("Write output");
			output = new FileWriter(args[3], true);
			output.write("\n" + "Graph: " +  args[1].substring(args[1].lastIndexOf('/')+1, args[1].length())
					+ "\tCover: " + args[2].substring(args[2].lastIndexOf('/')+1, args[2].length())
					+ "\tExtendedModularity= " + modularityValue);
			output.flush();
			output.close();
			
			break;
		case 2:
			/**
			 * NMI -inputPathGraph(DMIDFormat) -inputPathCommunities -inputPathGroundTruth
			 * -outputPath
			 */
			if (args.length != 5) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -inputPathCommunities -inputPathGroundTruth -outputPath");
			}

			cover = readCoverAndGraph(args[1], args[2]);
			
			CCSMatrix groundTruth =new CCSMatrix(cover.getGraph().vertexSet().size(),0);
			HashMap<Integer, Integer> cleanCommunityIDs = new HashMap<Integer,Integer>();
			br = null;
			try {
				br=new BufferedReader(new FileReader(args[3]));
				
				String line;
				JSONArray jsonNodeVector;
				JSONArray jsonMembershipVector;
				JSONArray jsonSingleMembership;

				int unusedCommunityID = 0;
				
				while ((line = br.readLine()) != null) {
					// [NodeID,[[CommunityID,MembershipDegree],...]]
					jsonNodeVector = new JSONArray(line);
					jsonMembershipVector = jsonNodeVector.getJSONArray(1);
					
					for (int i = 1; i < jsonMembershipVector.length(); ++i) {
						jsonSingleMembership = jsonMembershipVector.getJSONArray(i);
						
						if(!cleanCommunityIDs.containsKey(jsonSingleMembership.getInt(0))){
							cleanCommunityIDs.put(jsonSingleMembership.getInt(0), unusedCommunityID);

							groundTruth=(CCSMatrix)groundTruth.copyOfShape(groundTruth.rows(),unusedCommunityID+1);
							
							unusedCommunityID++;
							
						}
						groundTruth.set(jsonNodeVector.getInt(0), cleanCommunityIDs.get(jsonSingleMembership.getInt(0)), jsonSingleMembership.getDouble(1));
					}
				}
				
			} catch (IOException e) {
				System.out.println("Error Reading from the file. Exiting....");
				System.exit(0);
			
			} catch (JSONException e){
				System.out.println("Error Reading from the file. Exiting....");
				System.exit(0);
				
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
			cover.filterMembershipsbyThreshold(1);
			//filterMembershipsbyThreshold(1) for  groundtruth
			for(int i=0; i<groundTruth.rows(); i++) {
				
				Vector row = groundTruth.getRow(i);
				double rowThreshold = Math.min(row.fold(Vectors.mkMaxAccumulator()), 1);
				BelowThresholdEntriesVectorProcedure procedure = new BelowThresholdEntriesVectorProcedure(rowThreshold);
				row.each(procedure);
				List<Integer> belowThresholdEntries = procedure.getBelowThresholdEntries();
				for(int j : belowThresholdEntries) {
					row.set(j, 0);
				}
				groundTruth.setRow(i, row);
			}
			

			ExtendedNormalizedMutualInformationMetric nmiMetric = new ExtendedNormalizedMutualInformationMetric();
			double nmiValue = nmiMetric.measure(cover, groundTruth);

			output = new FileWriter(args[4], true);
			output.write("\n" + "Graph: " + args[1].substring(args[1].lastIndexOf('/')+1, args[1].length())+ "\tCover: " 
					+ args[2].substring(args[2].lastIndexOf('/')+1, args[2].length())
					+ "\tgroundTruth:" + args[3].substring(args[3].lastIndexOf('/')+1, args[3].length()) + "\tNMI= " + nmiValue);
			output.flush();
			output.close();

			break;
		case 3:
			/**
			 * ExtendedModularityNPNB08 -inputPathGraph(DMIDFormat) -inputPathCommunities
			 * -outputPath
			 */
			if (args.length != 4) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -inputPathCommunities -outputPath");
			}
			
			cover = readCoverAndGraph(args[1], args[2]);
			System.out.println("Calculate extended modularity");
			ExtendedModularityMetricNPNB08 modularityMetricNPNB08 = new ExtendedModularityMetricNPNB08();
			double modularityNPNB08Value = modularityMetricNPNB08
					.measure(cover);
			
			System.out.println("Write output");
			output = new FileWriter(args[3], true);
			output.write("\n" + "Graph: " +  args[1].substring(args[1].lastIndexOf('/')+1, args[1].length())
					+ "\tCover: " + args[2].substring(args[2].lastIndexOf('/')+1, args[2].length())
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
			graph = cleanBrokenIDs(graph,0);
			System.out.println("cleanBrokenIds finished");
			writeDMIDInputFormat(graph,args[2]);
			
			break;
		case 6:
			//"Expected arguments: -mode -inputPathGraph -outputPath -NumberIterations -inputThreshHold"
			String[] slpaArgs = new String[args.length-1];
			for(int i=1; i< args.length;++i){
				slpaArgs[i-1]=args[i];
			}
			SLPA.startSLPA(slpaArgs);
			break;
		case 7: 
			if (args.length != 3) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathCover(in Doca format) -outputPath");
			}
			try {
				br = new BufferedReader(new FileReader(args[1]));
				String line;
				int	unusedCommunityID=0;
				HashMap<Integer,Node> nodeMap= new HashMap<Integer,Node>();
				
				Node curNode;
				int nodeID=-1;
				int nextSpaceIndex;
				while ((line = br.readLine()) != null) {
					while(!line.isEmpty()){
						nextSpaceIndex =line.indexOf(' ')+1;
						
						if(nextSpaceIndex!=0){
							nodeID=Integer.parseInt(line.substring(0, nextSpaceIndex-1));
							line=line.substring(nextSpaceIndex);
						}else{
							nodeID=Integer.parseInt(line);
							line="";
						}
						if(nodeMap.containsKey(nodeID)){
							curNode=nodeMap.get(nodeID);
							curNode.addCommunity(unusedCommunityID, 1.0);
							
						}else{
							curNode=new Node(nodeID);
							curNode.addCommunity(unusedCommunityID, 1.0);
							nodeMap.put(nodeID, curNode);
						}
					}
					unusedCommunityID++;
				}
				output = new FileWriter(args[2], false);
				JSONArray jsonNode;
				JSONArray jsonMemberships;
				JSONArray jsonSingleDegree;
				int remainingNodes=nodeMap.size();
				for(Integer nodeIndex : nodeMap.keySet()){
					jsonNode= new JSONArray();
					jsonNode.put(nodeIndex);
					jsonMemberships=new JSONArray();
					for(Integer communityID : nodeMap.get(nodeIndex).getOwnCommunities().keySet()){
						jsonSingleDegree=new JSONArray();
						jsonSingleDegree.put(communityID);
						jsonSingleDegree.put(nodeMap.get(nodeIndex).getOwnCommunities().get(communityID));
						jsonMemberships.put(jsonSingleDegree);
					}
					jsonNode.put(jsonMemberships);
					output.write(jsonNode.toString());
					remainingNodes--;
					if(remainingNodes!=0){
						output.write("\n");
					}
				}
				output.flush();
				output.close();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			break;
		default:
			graph=readGraph(args[1]);
			graph=cleanBrokenIDs(graph,1);
			output =  new FileWriter(args[2], false);
			int currentEdge=0;
			
			for(Edge edge : graph.edgeSet()){
				/*if(graph.containsEdge(edge.getTarget(), edge.getSource())){
					//when edge is undirected only write the one (smallerID,biggerID)
					if(edge.getSource().getIndex()<edge.getTarget().getIndex()){
						output.write(edge.getSource().getIndex() +" "+ edge.getTarget().getIndex());
						if(currentEdge <graph.edgeSet().size()){
							output.write("\n");
						}
					}
				}else{*/
					output.write(edge.getSource().getIndex() +" "+ edge.getTarget().getIndex());
					if(currentEdge <graph.edgeSet().size()){
						output.write("\n");
					}
				//}
				currentEdge++;
			}
			output.flush();
			output.close();
			
			writeDMIDInputFormat(graph, "/home/hduser/Thesis/NetworksDMIDFormat/"+args[2].substring(args[2].lastIndexOf('/')+1, args[2].length()));
			
			
			break;
		}

	}

	private static SimpleDirectedWeightedGraph<Node, Edge> readGraph(
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
			if(line.startsWith("# Directed")){
				System.out.println("is directed");
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
				if(srcID != destID){
					graph.addEdge(srcNode, destNode);

					if(!isDirected){
						graph.addEdge(destNode, srcNode);
					}
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

	private static Cover readCoverAndGraph(	String inputPathGraph, String inputPathCommunities) {
		
		System.out.println("Start reading Cover: "+inputPathCommunities);
		SimpleDirectedWeightedGraph<Node, Edge> graph=new SimpleDirectedWeightedGraph<Node, Edge>(new EdgeFactoryDMID());
		
		HashMap<Integer, Integer> cleanCommunityIDs = new HashMap<Integer,Integer>();
		
		BufferedReader br = null;
		try {
			br=new BufferedReader(new FileReader(inputPathCommunities));
			
			String line;
	
			JSONArray jsonNodeVector;
			JSONArray jsonMembershipVector;
			JSONArray jsonSingleMembership;
			
	
			Node node;
			int unusedCommunityID = 0;
			
			while ((line = br.readLine()) != null) {
				// [NodeID,[[CommunityID,MembershipDegree],...]]
				jsonNodeVector = new JSONArray(line);
				node = new Node(jsonNodeVector.getInt(0));
				
				jsonMembershipVector = jsonNodeVector.getJSONArray(1);
				
				for (int i = 1; i < jsonMembershipVector.length(); ++i) {
					
					jsonSingleMembership = jsonMembershipVector.getJSONArray(i);
					
					if(!cleanCommunityIDs.containsKey(jsonSingleMembership.getInt(0))){
						
						cleanCommunityIDs.put(jsonSingleMembership.getInt(0), unusedCommunityID);
						unusedCommunityID++;
					}
					
					node.addCommunity(cleanCommunityIDs.get(jsonSingleMembership.getInt(0)), jsonSingleMembership.getDouble(1));
				}
				
				graph.addVertex(node);
			}
			
			br.close();
			System.out.println("Finished reading Cover: "+inputPathCommunities);
			
			br = new BufferedReader(new FileReader(inputPathGraph));
			System.out.println("Start reading Graph: "+inputPathGraph);
			
			JSONArray jsonNode;
			JSONArray jsonEdgeArray;
			JSONArray jsonSingleEdge;
			
			line = br.readLine();
			while(line!=null){
			
				jsonNode= new JSONArray(line);
				jsonEdgeArray = jsonNode.getJSONArray(1);
				
				int source = jsonNode.getInt(0);
				
				for(int i = 0 ; i<jsonEdgeArray.length();++i){
					
					jsonSingleEdge=jsonEdgeArray.getJSONArray(i);
					int dest=jsonSingleEdge.getInt(0);
					
					Node srcNode = new Node((int) source);
					Node destNode = new Node((int) dest);
					
					graph.addEdge(srcNode, destNode);
					
				}
				
				line = br.readLine();
			}
			
		} catch (IOException e) {
			System.out.println("Error Reading from the file. Exiting....");
			System.exit(0);
		
		} catch (JSONException e){
			System.out.println("Error Reading from the file. Exiting....");
			System.exit(0);
			
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		System.out.println("Creation of the graph complete.");
		
		Cover cover = new Cover(graph);
		Matrix memberships = new CCSMatrix(graph.vertexSet().size(),cleanCommunityIDs.size());
		
		System.out.println("Create membership matrix");
		HashMap<Integer, Double> communities;
		int nodeCounter=0;
		int numNodes =graph.vertexSet().size();
		for(Node node : graph.vertexSet()){
			communities = node.getOwnCommunities();
			for(Integer communityID : communities.keySet()){
				memberships.set(node.getIndex(), communityID,communities.get(communityID));	
			}
			node.setInDegree(graph.inDegreeOf(node));
			node.setOutDegree(graph.outDegreeOf(node));
			nodeCounter++;
			if(nodeCounter% 10000 == 0){
				System.out.println("processed node  "+nodeCounter+" / "+numNodes);
			}
		}
		nodeCounter=0;

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

	private static SimpleDirectedWeightedGraph<Node, Edge> cleanBrokenIDs(
			SimpleDirectedWeightedGraph<Node, Edge> graph, int startID) {
		Set<Node> nodeSet = graph.vertexSet();
		int numNodes = nodeSet.size();

		HashSet<Integer> unusedIDs = new HashSet<Integer>();
		for (int i = startID; i < numNodes+startID; ++i) {
			unusedIDs.add(new Integer(i));
		}
		System.out.println(startID);
			
		System.out.println("prepared UnusedIDs");
		System.out.println("NumNodes: "+numNodes);
		HashSet<Node> brokenNodes = new HashSet<Node>();
		for (Node node : nodeSet) {
			if ( node.getIndex() < numNodes && node.getIndex() >= startID) {
				unusedIDs.remove(new Integer(node.getIndex()));
			} else {
				brokenNodes.add(node);
			}
		}
		System.out.println("identified brokenNodes: "+ brokenNodes.size());
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
