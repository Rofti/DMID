package Main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.HashBiMap;

public class mainClass {

	private static boolean DEBUG = false;

	public static void main(String[] args) throws NumberFormatException,
			IOException {

		String line;
		Long numNodes = new Long(0);
		Long numEdges = new Long(0);

		HashMap<Long, HashSet<Long>> nodes = new HashMap<Long, HashSet<Long>>();
		HashSet<Long> edges;
		HashBiMap<Long, Long> mapping = HashBiMap.create();

		switch (args.length) {
		case 3:
			/** -inputPath -outPutGraphpath -OutputMappingPath */
			BufferedReader br = new BufferedReader(new FileReader(args[0]));

			Long srcID;
			Long destID;
			Long curID = new Long(0);

			while ((line = br.readLine()) != null) {
				if (line.startsWith("# Nodes:")) {

					line = line.substring(9, line.length());
					numNodes = Long.parseLong(line.substring(0,
							line.indexOf(" ")));

					line = line.substring(line.lastIndexOf(" ") + 1,
							line.length());
					numEdges = Long.parseLong(line);
				} else if (!line.startsWith("#")) {
					line = line.replace("\t", " ");
					srcID = Long
							.parseLong(line.substring(0, line.indexOf(" ")));
					destID = Long.parseLong(line.substring(
							line.indexOf(" ") + 1, line.length()));

					if (!mapping.containsKey(srcID)) {
						mapping.put(srcID, curID);
						curID++;
					}
					if (!mapping.containsKey(destID)) {
						mapping.put(destID, curID);
						curID++;
					}

					if (!nodes.containsKey(mapping.get(srcID))) {
						edges = new HashSet<Long>();
						edges.add(mapping.get(destID));
						nodes.put(mapping.get(srcID), edges);
					} else {

						edges = nodes.get(mapping.get(srcID));
						edges.add(mapping.get(destID));
						nodes.put(mapping.get(srcID), edges);
					}

				}
			}

			br.close();

			/** create graph output */
			Long curMappedID = new Long(-1);
			FileWriter graphOutput = new FileWriter(args[1]);

			for (Entry<Long, HashSet<Long>> singleNode : nodes.entrySet()) {
				JSONArray jsonVertex = new JSONArray();
				JSONArray jsonEdges = new JSONArray();
				JSONArray jsonSingleEdge = new JSONArray();
				try {
					curMappedID = singleNode.getKey();
					jsonVertex.put(singleNode.getKey());

					for (Long singleEdge : singleNode.getValue()) {
						jsonSingleEdge= new JSONArray();
						jsonSingleEdge.put(singleEdge);
						jsonSingleEdge.put(1.0);
						jsonEdges.put(jsonSingleEdge);
					}
					jsonVertex.put(jsonEdges);

				} catch (Exception e) {
					String out = "Graph Output: Couldn't write node! original node id: "
							+ mapping.inverse().get(curMappedID)
							+ " new (mapped) id: " + curMappedID;
					System.out.println(out);
				}

				graphOutput.write(jsonVertex.toString());
				graphOutput.write("\n");

			}

			graphOutput.flush();
			graphOutput.close();

			/** create MapOutput */
			FileWriter mapOutput = new FileWriter(args[2]);
			mapOutput.write("# [originalNodeID, newNodeID]");
			mapOutput.write("\n");

			for (Entry<Long, Long> mappedNode : mapping.entrySet()) {
				JSONArray jsonVertex = new JSONArray();
				jsonVertex.put(mappedNode.getKey());
				jsonVertex.put(mappedNode.getValue());

				mapOutput.write(jsonVertex.toString());
				mapOutput.write("\n");
			}

			mapOutput.flush();
			mapOutput.close();

			if (DEBUG) {
				debug(numNodes, nodes, mapping);
			}

			break;
			
		case 4:
			if (!args[0].equalsIgnoreCase("-rm")) {
				System.out.println("Unsupported input arguments. Check -help.");
				break;
			}

			BufferedReader bufferR = new BufferedReader(new FileReader(args[2]));
			mapping = HashBiMap.create();

			while ((line = bufferR.readLine()) != null) {
				if (line.startsWith("[")) {
					JSONArray jsonMap;
					try {
						jsonMap = new JSONArray(line);
						mapping.put(jsonMap.getLong(1), jsonMap.getLong(0));

					} catch (JSONException e) {
						e.printStackTrace();
						System.out.println("Could not read inputMap. Line: "
								+ line);
					}

				}
			}

			HashMap<Long, HashMap<Long, Double>> cover = new HashMap<Long, HashMap<Long, Double>>();

			bufferR.close();
			bufferR = new BufferedReader(new FileReader(args[1]));

			while ((line = bufferR.readLine()) != null) {
				if (line.startsWith("[")) {
					JSONArray jsonMembershipDeg;
					JSONArray jsonMember;
					try {

						jsonMembershipDeg = new JSONArray(line);
						jsonMember = jsonMembershipDeg.getJSONArray(1);
						HashMap<Long, Double> member = new HashMap<Long, Double>();
						// [1,[[1,1/4],[2,0.0]]]
						for (int i = 0; i < jsonMember.length(); ++i) {
							JSONArray communityEntry = jsonMember
									.getJSONArray(i);
							member.put(mapping.get(communityEntry.getLong(0)),
									communityEntry.getDouble(1));
						}
						cover.put(mapping.get(jsonMembershipDeg.getLong(0)),
								member);

					} catch (JSONException e) {
						e.printStackTrace();
						System.out.println("Could not read inputCover. Line: "
								+ line);
					}
				}
				
			}
			bufferR.close();
			FileWriter newCoverOutput = new FileWriter(args[3]);
			
			for (Entry<Long, HashMap<Long,Double>> entryCover : cover.entrySet()) {
				
				JSONArray jsonVertex = new JSONArray();
				JSONArray jsonAllDegree = new JSONArray();
				JSONArray jsonSingleDegree = new JSONArray();
				
				for(Entry<Long,Double> entryMember: entryCover.getValue().entrySet()){
					
					jsonSingleDegree = new JSONArray();
					jsonSingleDegree.put(entryMember.getKey());
					jsonSingleDegree.put(entryMember.getValue());
					
					jsonAllDegree.put(jsonSingleDegree);
				}
				jsonVertex.put(entryCover.getKey());
				jsonVertex.put(jsonAllDegree);
				
				newCoverOutput.write(jsonVertex.toString());
				newCoverOutput.write("\n");
			}
			

			newCoverOutput.flush();
			newCoverOutput.close();
			bufferR.close();
			
			break;
		default:
			String help = "args: <SNAPInputPath> <GraphOutputPath> <MappingOutputPath> \n"
					+ "or: -rM <DMIDCommunityCoverInputPath> <MappingInputPath> <NewCoverOutputPath>"
					+ "\tFormats SNAPEdgeInputFormat to a valid DMIDVertexInputFormat \n"
					+ "\tAll files are located in \\InputPreprocessing \n"
					+ "\tInput: txt file in snap format \n"
					+ "\tOutput: The graph in a new Format and the vertx index mapping: [newIndex,oldIndex]";
			System.out.println(help);
			break;
		}

	}

	private static void debug(Long numNodes,
			HashMap<Long, HashSet<Long>> nodes, HashBiMap<Long, Long> mapping) {
		System.out.println("Nodes:");
		for (Entry<Long, HashSet<Long>> singleNode : nodes.entrySet()) {
			String out = "[" + singleNode.getKey() + ",[";

			for (Long singleEdge : singleNode.getValue()) {
				out += "," + singleEdge;
			}
			out += "]]";
			System.out.println(out);
		}
		System.out.println("Mapping:");
		for (Entry<Long, Long> mappedNode : mapping.entrySet()) {
			String out = mappedNode.getKey() + " --> " + mappedNode.getValue();

			System.out.println(out);
		}

		if (nodes.size() != numNodes) {
			System.out
					.println("WARNING: Unexpected number of nodes!\n\t nodes found: "
							+ nodes.size() + "\n\t nodes expected: " + numNodes);
		}
	}
}
