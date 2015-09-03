package ocd.algorithm.SLPA;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * Data structure the hold the undirected graph on which clustering algorithm is run to
 * find the overlapping communities using SLPA algorithm.
 * @author pejakalabhargava
 *
 */
public class Graph {
	
	//Number of edges in the graph
	//int edges;
	
	//Number of vertices of the graph
	//int vertices;
	
	//This holds the id of the node and the reference to corresponding Node reference
	Map<Integer, Node> graphADT;
	
	// This is used to hold the final communities that are calculated.Key of the
	// map is the labelId of the community and value is the set of integers
	// representing the node Id's present in that community.
	Map<Integer,Set<Integer>> overlappingCommunities;

	// Constructor to create the graph which takes the filename as input.
	// First line consists of number of vertices and number of edges separated by
	// space.Subsequent lines consists of edges in the network represented as
	// "from to".
	public Graph(String filepath) {
		graphADT = new LinkedHashMap<Integer, Node>();
		overlappingCommunities =  new HashMap<Integer, Set<Integer>>();
		readGraph(filepath);
	}

	/**
	 * Reads DMIDinputFormat
	 * @param filepath
	 */
	private void readGraph(String filepath) {
		System.out.println("Reading from the input file and creating graph...");
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
					
					addEdge(source, dest);
				}
				
				line = br.readLine();
			}
			
			System.out.println("Number of vertices are:" + getNumberVertices());

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
	}

	/**
	 * This method adds an edge into the graph data structure.make sure to add two entries since the graph is
	 * undirected.
	 * @param source
	 * @param dest
	 */
	private void addEdge(Integer source, Integer dest) {
		//Check if source node exists else create a new one.
		Node sourceNode = graphADT.get(source);
		if (sourceNode == null) {	
			sourceNode = new Node(source);
			graphADT.put(source, sourceNode);
		}
		//Check if destintation node exists else create a new one.
		Node destNode = graphADT.get(dest);
		if (destNode == null) {
			destNode = new Node(dest);
			graphADT.put(dest, destNode);
		}
		//Add an entry into the adjacenecy list.
		sourceNode.addNeighbour(destNode);
		destNode.addNeighbour(sourceNode);
	}

	/**
	 * Gets the node based on nodeId
	 * @param nodeId
	 * @return integer
	 */
	public Node getNode(int nodeId) {
		return graphADT.get(nodeId);
	}


	/**
	 * Gets number of vertices in the undirected graph
	 * @return integer
	 */
	public int getNumberVertices() {
		return graphADT.size();
	}

	/**
	 * Returns the map holding the final communities.
	 * @return community map
	 */
	public Map<Integer, Set<Integer>> getOverlappingCommunities() {
		return overlappingCommunities;
	}
}
