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
import ocd.metrics.utils.NonZeroEntriesVectorProcedure;
import ocd.algorithm.RAWLPA.RandomWalkLabelPropagationAlgorithm;
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
import org.la4j.matrix.dense.Basic2DMatrix;
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
			 * NMI -inputPathGraph(DMIDFormat) -inputPathCommunities -inputPathGroundTruth -inputpath brokenNodelist
			 * -outputPath
			 */
			if (args.length != 6) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathGraph -inputPathCommunities -inputPathGroundTruth -outputPath");
			}

			cover = readCoverAndGraph(args[1], args[2]);
			
			br=new BufferedReader(new FileReader(args[4]));
			HashMap <Integer, Integer> brokenNodes = new HashMap<Integer, Integer>();
			String line;
			int brokenId;
			int fixedId;
			while ((line = br.readLine()) != null) {
				brokenId = Integer.parseInt(line.substring(0, line.indexOf(" ")));
				fixedId = Integer.parseInt(line.substring(line.indexOf(" ") + 1,
						line.length()));
				brokenNodes.put(brokenId,fixedId);
			}
			br.close();
			System.out.println("start reading groundtruth.");
			CCSMatrix groundTruth =new CCSMatrix(cover.getGraph().vertexSet().size(),cover.getMemberships().columns());
			HashMap<Integer, Integer> cleanCommunityIDs = new HashMap<Integer,Integer>();
			try {
				br=new BufferedReader(new FileReader(args[3]));
				

				JSONArray jsonNodeVector;
				JSONArray jsonMembershipVector;
				JSONArray jsonSingleMembership;

				int unusedCommunityID = 0;
				int counter=0;
				while ((line = br.readLine()) != null) {
					// [NodeID,[[CommunityID,MembershipDegree],...]]
					jsonNodeVector = new JSONArray(line);
					jsonMembershipVector = jsonNodeVector.getJSONArray(1);
					
					for (int i = 1; i < jsonMembershipVector.length() 
							&&(brokenNodes.containsKey(jsonNodeVector.getInt(0))
									|| jsonNodeVector.getInt(0)<groundTruth.rows())
							; ++i) {
						jsonSingleMembership = jsonMembershipVector.getJSONArray(i);
						
						if(!cleanCommunityIDs.containsKey(jsonSingleMembership.getInt(0))){
							cleanCommunityIDs.put(jsonSingleMembership.getInt(0), unusedCommunityID);
							if(unusedCommunityID>=groundTruth.columns()){
								groundTruth=(CCSMatrix)groundTruth.copyOfShape(groundTruth.rows(),unusedCommunityID+1);
							}
							unusedCommunityID++;
							
						}
						
						if(brokenNodes.containsKey(jsonNodeVector.getInt(0))){
							groundTruth.set(brokenNodes.get(jsonNodeVector.getInt(0)), cleanCommunityIDs.get(jsonSingleMembership.getInt(0)), jsonSingleMembership.getDouble(1));

						}else {
							groundTruth.set(jsonNodeVector.getInt(0), cleanCommunityIDs.get(jsonSingleMembership.getInt(0)), jsonSingleMembership.getDouble(1));

						}
					}

					if(counter%10000==0){
						System.out.println("processed nodes: "+counter+" / "+cover.getGraph().vertexSet().size());
					}
					counter++;
				}
				if(unusedCommunityID<groundTruth.columns()){
					groundTruth=(CCSMatrix)groundTruth.copyOfShape(groundTruth.rows(),unusedCommunityID);
				}
				if(groundTruth.getColumn(groundTruth.columns()-1).infinityNorm()==0){
					System.out.println("Error Reading from groundtruth....");
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
			System.out.println("finished reading groundtruth.");
			System.out.println("filter illegal membership degrees in cover.");
			//cover.filterMembershipsbyThreshold(1);
			//filterMembershipsbyThreshold(1) for  groundtruth
			/*System.out.println("filter illegal membership degrees in groundtruth.");
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
			*/
			
			ExtendedNormalizedMutualInformationMetric nmiMetric = new ExtendedNormalizedMutualInformationMetric();
			double nmiValue = nmiMetric.measure(cover, groundTruth);

			output = new FileWriter(args[5], true);
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
			String networkName=args[1].substring(args[1].lastIndexOf('/')+1, args[1].length());
			graph = readDMIDInputFormat(args[1]);
			spearmanMeasure(graph, args[2], networkName);
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
				int	unusedCommunityID=0;
				Set<Node> nodeSet= new HashSet<Node>();
				
				Node curNode;
				int nodeID=-1;
				int nextSpaceIndex;
				while ((line = br.readLine()) != null) {
					while(!line.isEmpty()){
						nextSpaceIndex =line.indexOf('\t')+1;
						
						if(nextSpaceIndex!=0){
							nodeID=Integer.parseInt(line.substring(0, nextSpaceIndex-1));
							line=line.substring(nextSpaceIndex);
						}else{
							nodeID=Integer.parseInt(line);
							line="";
						}
						curNode=new Node(nodeID);
						if(nodeSet.contains(new Node(nodeID))){
							curNode.addCommunity(unusedCommunityID, 1.0);
							
						}else{
							curNode.addCommunity(unusedCommunityID, 1.0);
							nodeSet.add(curNode);
						}
					}
					unusedCommunityID++;
				}
				
				output = new FileWriter(args[2], false);
				JSONArray jsonNode;
				JSONArray jsonMemberships;
				JSONArray jsonSingleDegree;
				int remainingNodes=nodeSet.size();
				for(Node node : nodeSet){
					jsonNode= new JSONArray();
					jsonNode.put(node.getIndex());
					jsonMemberships=new JSONArray();
					for(Integer communityID : node.getOwnCommunities().keySet()){
						jsonSingleDegree=new JSONArray();
						jsonSingleDegree.put(communityID);
						jsonSingleDegree.put(node.getOwnCommunities().get(communityID));
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
		case 8:
			if (args.length != 4) {
				throw new IllegalArgumentException(
						"Expected arguments: -mode -inputPathgraph -outputPathCover -outputPathRuntime");
			}
			networkName=args[1].substring(args[1].lastIndexOf('/')+1, args[1].length());
			
			graph=readDMIDInputFormat(args[1]);
			//graph=cleanBrokenIDs(graph,0);
		
			//Time in milliseconds when algorithm is started (after reading input)
			long startTime = System.currentTimeMillis();		
			
			System.out.println("Starting RAWLPA");
			RandomWalkLabelPropagationAlgorithm rawLpa = new RandomWalkLabelPropagationAlgorithm();
			cover = rawLpa.detectOverlappingCommunities(graph);
			
			//Time in milliseconds when algorithm terminates (before writing output)
			long endTime = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			
			System.out.println("Ending the RAWLPA \nRun-Time in seconds: "+ (totalTime/1000) +"\n");
			
			Matrix memberships=cover.getMemberships();
			
			output = new FileWriter(args[2], false);
			JSONArray jsonNode; // [NodeID, jsonCommunityArray]
			JSONArray jsonCommunityArray; // [JsonMemDeg_1,...,JsonMemDeg_n] only those with MemDeg>0 
			JSONArray jsonMemDeg; // [Community ID , Membership Degree] 
			
			for(int i =0; i < memberships.rows();++i){
				jsonNode=new JSONArray();
				jsonNode.put(i);
				
				NonZeroEntriesVectorProcedure procedure = new NonZeroEntriesVectorProcedure();
				memberships.getRow(i).each(procedure);
				List<Integer> nonZeroEntries = procedure.getNonZeroEntries();
				
				jsonCommunityArray=new JSONArray();
				for(int j : nonZeroEntries) {

					jsonMemDeg=new JSONArray();
					jsonMemDeg.put(j);
					jsonMemDeg.put(memberships.get(i,j));
					jsonCommunityArray.put(jsonMemDeg);
				}
				jsonNode.put(jsonCommunityArray);
				
				output.write(jsonNode.toString());
				if(i != memberships.rows()-1){
					output.write("\n");
				}
			}
			output.flush();
			output.close();
			
			output = new FileWriter(args[3], true);
			output.write(networkName+"\t RunTime(seconds):  "+(totalTime/1000)+"\n");
			output.flush();
			output.close();
			
			break;
		case 9: 
			br = new BufferedReader(new FileReader(args[1]));
			int srcID;
			int destID;
			graph=new SimpleDirectedWeightedGraph<Node, Edge>(new EdgeFactoryDMID());
			boolean isDirected=false;
			
			while ((line = br.readLine()) != null) {
				if(line.startsWith("  directed ")){
					line=line.substring(line.lastIndexOf(" ")+1,line.length() );
					isDirected=(Integer.parseInt(line)==1);
					System.out.println("isDirected: "+isDirected);
				}
				if(line.startsWith("    source ")){
					line=line.substring(line.lastIndexOf(" ")+1,line.length() );
					srcID = Integer.parseInt(line);
					
					line=br.readLine();
					line=line.substring(line.lastIndexOf(" ")+1,line.length() );
					destID = Integer.parseInt(line);
					
					if(destID!=srcID){
						graph.addVertex(new Node(srcID));
						graph.addVertex(new Node(destID));
						graph.addEdge(new Node(srcID), new Node(destID));
						if(!isDirected){
							graph.addEdge( new Node(destID),new Node(srcID));
						}
					}
				}
			}
			System.out.println("numNodes: "+graph.vertexSet().size() +"\t"+"numEdges: "+graph.edgeSet().size());
			
			graph=cleanBrokenIDs(graph, 0);
			
			writeDMIDInputFormat(graph, args[2]);
			
			networkName=args[1].substring(args[1].lastIndexOf('/')+1, args[1].length());
			output = new FileWriter("/home/hduser/Thesis/UCINet/NodesAndEdges.txt", true);
			output.write(networkName+"\t Nodes:  "+graph.vertexSet().size()+"\t Edges: "+(isDirected? graph.edgeSet().size() :graph.edgeSet().size()/2)+"\n");
			output.flush();
			output.close();
			break;
		default:
			graph=readGraph(args[1]);
			graph=cleanBrokenIDs(graph,1);
			output =  new FileWriter(args[2], false);
			int currentEdge=1;
			
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
				if(srcID != destID){
					graph.addVertex(srcNode);
					graph.addVertex(destNode);

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

	private static Cover readCoverAndGraph(	String inputPathGraph, String inputPathCommunities) throws IOException, JSONException {
		
		System.out.println("Start reading Cover: "+inputPathCommunities);
		
		Cover cover = new Cover(new SimpleDirectedWeightedGraph<Node, Edge>(new EdgeFactoryDMID()));
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
				
				for (int i = 0; i < jsonMembershipVector.length(); ++i) {
					
					jsonSingleMembership = jsonMembershipVector.getJSONArray(i);
					
					if(!cleanCommunityIDs.containsKey(jsonSingleMembership.getInt(0))){
						
						cleanCommunityIDs.put(jsonSingleMembership.getInt(0), unusedCommunityID);
						unusedCommunityID++;
					}
					
					node.addCommunity(cleanCommunityIDs.get(jsonSingleMembership.getInt(0)), jsonSingleMembership.getDouble(1));
				}
				
				cover.getGraph().addVertex(node);
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

					cover.getGraph().addEdge(srcNode, destNode);

					
				}
				
				line = br.readLine();
			}
			
		/*} catch (IOException e) {
			System.out.println("Error Reading from the file. Exiting....");
			System.exit(0);
		
		} catch (JSONException e){
			System.out.println("Error Reading from the file. Exiting....");
			System.exit(0);
			*/
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
		
		
		Matrix memberships = new /*CCSMatrix*/Basic2DMatrix(cover.getGraph().vertexSet().size(),cleanCommunityIDs.size());
		
		Node nodeFixed=new Node(0);
		Node brokenNode=new Node(memberships.rows());
		for(Node node: cover.getGraph().vertexSet()){
			if(node.getIndex()==memberships.rows()){
			  brokenNode=node;
			}
		}
		System.out.println("Create membership matrix");
		nodeFixed.setOwnCommunities(brokenNode.getOwnCommunities());
		cover.getGraph().addVertex(nodeFixed);
		for(Edge edge:cover.getGraph().outgoingEdgesOf(brokenNode)){
			cover.getGraph().addEdge(nodeFixed, edge.getTarget());
		}
		for(Edge edge:cover.getGraph().incomingEdgesOf(brokenNode)){
			cover.getGraph().addEdge(edge.getSource(), nodeFixed);
		}
		cover.getGraph().removeVertex(brokenNode);
		

		HashMap<Integer, Double> communities;
		int nodeCounter=0;
		int numNodes =cover.getGraph().vertexSet().size();
		for(Node node : cover.getGraph().vertexSet()){
			communities = node.getOwnCommunities();
			for(Integer communityID : communities.keySet()){
				memberships.set(node.getIndex(), communityID,communities.get(communityID));	
			}
			node.setInDegree(cover.getGraph().inDegreeOf(node));
			node.setOutDegree(cover.getGraph().outDegreeOf(node));
			nodeCounter++;
			if(nodeCounter% 100000 == 0){
				System.out.println("processed node  "+nodeCounter+" / "+numNodes);
			}
		}

		cover.setMemberships(memberships);
		return cover;
	}
	
	public static SimpleDirectedWeightedGraph<Node, Edge>  readDMIDInputFormat(String filepath){
		System.out.println("Reading from the input file and creating graph...");
		SimpleDirectedWeightedGraph<Node, Edge>  graph = new SimpleDirectedWeightedGraph<Node, Edge> (new EdgeFactoryDMID());
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filepath));
			String line;

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
					
					graph.addVertex(new Node(source));
					graph.addVertex(new Node(dest));
					graph.addEdge(new Node(source), new Node(dest));
				}
				if(jsonEdgeArray.length()==0){
					graph.addVertex(new Node(source));
				}
				line = br.readLine();
			}
			
			System.out.println("Number of vertices are:" +graph.vertexSet().size());

		} catch (IOException e) {
			System.out.println("Error Reading from the file.Exiting....");
			System.exit(0);
		
		} catch (JSONException e){
			System.out.println("Error Reading from the file. File not in DMID-InputFormat. Exiting....");
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
		return graph;
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
			SimpleDirectedWeightedGraph<Node, Edge> graph, String outputPath,String networkName)
			throws IOException {

		double[] dataX = new double[graph.edgeSet().size()];
		double[] dataY = new double[graph.edgeSet().size()];

		Edge[] edges = graph.edgeSet()
				.toArray(new Edge[graph.edgeSet().size()]);

		for (int i = 0; i < graph.edgeSet().size(); ++i) {

				dataX[i] = graph.outDegreeOf(edges[i].getSource()) + Math.random();
				dataY[i] = graph.inDegreeOf(edges[i].getTarget()) + Math.random();
			
		}

		RankingAlgorithm natural = new NaturalRanking(NaNStrategy.MINIMAL,
				TiesStrategy.SEQUENTIAL);
		SpearmansCorrelation spearmansCor = new SpearmansCorrelation(natural);
		double spearmanRank = spearmansCor.correlation(dataX, dataY);

		FileWriter output = new FileWriter(outputPath, true);
		output.write("\n" + networkName + "\t Spearmans rho= " + spearmanRank
				+ "\t Edges=" + graph.edgeSet().size() + "\t Nodes= "
				+ graph.vertexSet().size());
		output.flush();
		output.close();
	}

	private static SimpleDirectedWeightedGraph<Node, Edge> cleanBrokenIDs(
			SimpleDirectedWeightedGraph<Node, Edge> graph, int startID) throws IOException {
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
		FileWriter output =  new FileWriter("BrokenNodeList.txt", false);
		int counter=1;
		for (Node nodeB : brokenNodes) {
			
			nodeSet = graph.vertexSet();

			iter = unusedIDs.iterator();
			id = iter.next();

			unusedIDs.remove(id);
			fixedNode = new Node(id);
			graph.addVertex(fixedNode);
			
			output.write(nodeB.getIndex() +" "+ id);
			if(counter <brokenNodes.size()){
				output.write("\n");
			}
			outgoingB = graph.outgoingEdgesOf(nodeB);
			incomingB = graph.incomingEdgesOf(nodeB);
			
			for (Edge edge : outgoingB) {
				graph.addEdge(fixedNode, edge.getTarget());
			}

			for (Edge edge : incomingB) {
				graph.addEdge(edge.getSource() ,fixedNode);
			}

			graph.removeVertex(nodeB);
			counter++;
		}
		output.flush();
		output.close();
		System.out.println("remaining unused ids: "+ unusedIDs.size());
		return graph;
	}

}
