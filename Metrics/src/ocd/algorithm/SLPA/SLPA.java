package ocd.algorithm.SLPA;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.json.JSONArray;

/**
 * This project is implementation of Speaker-listener Label Propagation Algorithm
 * which is used to detecting overlapping community.
 * Algorithm:SLPA(T, r )
 *	T : the user defined maximum iteration
 *	r: post-processing threshold
 *	1)	First, the memory of each node is initialized with a unique label.
 *	2)	Then, the following steps are repeated until the maximum iteration T is reached:
 *		a. 	One node is selected as a listener.
 *		b. 	Each neighbor of the selected node randomly selects a label with probability
 *			proportional to the occurrence frequency of this label in its memory and sends
 *			the selected label to the listener.
 *		c. The listener adds the most popular label received to its memory.
 *	3)	Finally, the post-processing based on the labels in the memories and the threshold
 *		r is applied to output the communities.
 * 
 * Run as: 	mvn clean install
 * 		 	mvn exec:java -Dexec.args="amazon.graph.original amazon.graph.comm 20 0.5" from command line.
 * 		    or
 * 		   	mvn clean install
 * 			cd target
 * 			java -jar slpa-0.0.1.jar amazon.graph.original amazon.graph.comm 20 0.5
 * 
 * @author pejakalabhargava
 *
 */
public class SLPA {

	//This holds the input graph 
	Graph graph;
	
	//r: post-processing threshold
	double threshHold;
	
	//T : the user defined maximum iteration
	int iterations;
	
	//Output file to store the communities
	String outputFile;

	/*
	 * Constructor to create the graph from the given input.
	 */
	public SLPA(String inputFileName, String outPutFileName,
			int inputIterations, double inputThreshHold) {
		threshHold = inputThreshHold;
		iterations = inputIterations;
		outputFile = outPutFileName;
		graph = new Graph(inputFileName);
	}
	
	public static void startSLPA(String[] args) {
		//Check if there are 4 arguments input inputFileName
		if (args.length != 4) {
			System.out.println("Invalid number of arguments."
					+ "Expected: -mode -inputFileName -outputPath -NumberIterations -inputThreshHold");
			System.exit(0);
		}
		System.out.println("\nStarting the Speaker-listener Label Propagation Algorithm to find overlapping communities");
		String inputFileName = args[0];
		String networkName=args[0].substring(args[0].lastIndexOf('/')+1, args[0].length());
		System.out.println("Input file name is:" + networkName);
		networkName=networkName.replaceAll(".txt", "");
		
		int inputIterations = Integer.parseInt(args[2]);
		System.out.println("Number of iterations are:" + inputIterations);
		Double inputThreshHold = Double.parseDouble(args[3]);

		System.out.println("Threshold is:" + inputThreshHold);

		String outPutFileName = args[1]+networkName+"_SLPA_"+inputIterations+"_"+inputThreshHold.toString().replaceAll("0.", "")+".txt";
		
		SLPA algorithm = new SLPA(inputFileName, outPutFileName,
				inputIterations, inputThreshHold);
		//Time in milliseconds when algorithm is started (after reading input)
		long startTime = System.currentTimeMillis();		
		
		//Step 2 of the SLAP algorithm where memory labels are updated on each iteration
		System.out.println("Propogating the memory labels....");
		algorithm.propogateMemorylabel();
		// Step 3:post-processing based on the labels in the memories and the
		// threshold r is applied to output the communities
		System.out.println("Post-Processing to apply threshold " + inputThreshHold + " to output communities");
		algorithm.postProcessing();
		
		//Time in milliseconds when algorithm terminates (before writing output)
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		//Save the output file 
		System.out.println("Saving the communities to output file:" + outPutFileName);
		algorithm.outputOverlappingCommunitiesToFile();
		System.out.println("Ending the Speaker-listener Label Propagation Algorithm\nRun-Time in seconds: "+ (totalTime/1000) +"\n");
		
		FileWriter writeRunTime=null;
		try {
			writeRunTime = new FileWriter(args[1]+networkName+"_SLPA_RunTime.txt", true);
			
			writeRunTime.write(networkName+"\tIteration:  "+inputIterations +"\tThreshold:  "+inputThreshHold +"\t RunTime(seconds):  "+(totalTime/1000)+"\n");
		} catch (IOException e) {
			System.err.println("Error writing the SLPA RunTime file : ");
			e.printStackTrace();
		}finally {
			if (writeRunTime != null) {
				try {
					writeRunTime.close(); 
					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
	
	/**
	 * The following steps are repeated until the maximum iteration T is reached:
	 *	a. 	One node is selected as a listener.
	 *	b. 	Each neighbor of the selected node randomly selects a label with probability
	 *		proportional to the occurrence frequency of this label in its memory and sends
	 *		the selected label to the listener.
	 *	c. The listener adds the most popular label received to its memory.
	 */
	private void propogateMemorylabel() {
		
		//Create an array to hold all the nodeIds from 0 to (noOfVertices-1)
		int[] nodeId = new int[graph.getNumberVertices()];
		//Initilaize the array with values from 0 to (noOfVertices-1)
		for (int i = 0; i < nodeId.length; i++) {
			nodeId[i] = i;
		}
		
		//Loop iteration T number of times
		for (int i = 1; i <=iterations; i++) {
			System.out.println("starting Iteration " + i + " of SLPA.");
			// Rearrange the integer array to hold numbers in random order from
			// 0 to (noOfVertices-1) in it.
			ShuffleArray(nodeId);
			for (int j = 0; j < nodeId.length; j++) {
				//One node is selected as a listener
				Node listener = graph.getNode(j);
				// Call listen so that all neighbours can speak to this node by
				// sending selected label.
				if(listener == null){
					System.out.println("NodeID missing: "+j);
				}
				listener.listen();
			}
		}
	}
	
	/**
	* Fisher–Yates shuffle(http://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle)
	* function to shuffle the elements of a given integer array.
	*/
	private void ShuffleArray(int[] array)
	{
	    int index;
	    Random random = new Random();
	    for (int i = array.length - 1; i > 0; i--)
	    {
	        index = random.nextInt(i + 1);
	        if (index != i)
	        {
	            array[index] ^= array[i];
	            array[i] ^= array[index];
	            array[index] ^= array[i];
	        }
	    }
	}
	
	/**
	 * This function implements the post-processing based on the labels in the
	 * memories and the threshold r is applied to output the communities
	 */
	private void postProcessing() {
		
		//Get the map to hold the community label and set of nodes in that community
		Map<Integer, Set<Integer>> community = graph.getOverlappingCommunities();
		//find the cluster each node of the graph belongs to.
		for (int i = 0; i < graph.getNumberVertices(); i++) {
			Node node = graph.getNode(i);
			//Get the memory map of the node with label as key
			//and count as value
			Map<Integer, Integer> memoryMap = node.getMemoryMap();
			//get the number of communities this node belongs to
			int noOfCommunities = node.getNoOfCommunities();
			//Iterate through the memory map
			for (Map.Entry<Integer, Integer> entry : memoryMap.entrySet()) {
				Integer labelId = entry.getKey();
				Integer count = entry.getValue();
				// Calculate the ratio of the label count against total number
				// of communities 
				double probalityDensity = (double)count/noOfCommunities;
				//If ration is greater than threhsold input, then add this node to the community
				//identified by the label.
				if(probalityDensity >= threshHold) {
					//Check if the label exits in the community map and if it doesnt
					//exist create one for this label and add the node to the set.
					if(community.get(labelId) == null) { 
						Set<Integer> communitySet = new HashSet<Integer>();
						community.put(labelId,communitySet);
						communitySet.add(node.getNodeId());
					} else {
						Set<Integer> communitySet = community.get(labelId);
						communitySet.add(node.getNodeId());
					}
				}
			}
		}
	}
	
	/**
	 * This method is used to ouput the communities to an output file.
	 */
	private void outputOverlappingCommunitiesToFile() {
		Writer fileWriter = null;
		BufferedWriter bufferedWriter = null;
		try {
			File file = new File(outputFile);
			fileWriter = new FileWriter(file);
			bufferedWriter = new BufferedWriter(fileWriter);
			
			//This consists of map of label and set of nodes in the community identified by the label.
			Map<Integer, Set<Integer>> communities = graph.getOverlappingCommunities();

			JSONArray jsonNode; // [NodeID, jsonCommunityArray]
			JSONArray jsonCommunityArray; // [JsonMemDeg_1,...,JsonMemDeg_n] only those with MemDeg>0 
			JSONArray jsonMemDeg; // [Community ID , Membership Degree] 
			
			int remainingNodes = graph.graphADT.size();
			System.out.println("There are totally " + communities.size() + " communities detected.");
			System.out.println("Writing Output...");
			for(Entry<Integer, Set<Integer>> entry : communities.entrySet()){
				int communityID=entry.getKey();
				for(Integer nodeID : entry.getValue()){
					graph.graphADT.get(nodeID).addCommunity(communityID);
				}
			}
			for(Node node : graph.graphADT.values()){
				remainingNodes--;
				jsonNode=new JSONArray();
				jsonNode.put(node.getNodeId());
				
				jsonCommunityArray=new JSONArray();

				for(Integer communityID : node.getCommunities()){
					jsonMemDeg=new JSONArray();
					jsonMemDeg.put(communityID);
					jsonMemDeg.put(1);
					jsonCommunityArray.put(jsonMemDeg);
					
				}
				
				jsonNode.put(jsonCommunityArray);
				
				bufferedWriter.write(jsonNode.toString());
				
				if(remainingNodes!=0){
					bufferedWriter.newLine();
				}
			}

		} catch (IOException e) {
			System.err.println("Error writing the SLPA output file : ");
			e.printStackTrace();
		} finally {
			if (bufferedWriter != null && fileWriter != null) {
				try {
					bufferedWriter.close();
					fileWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
