import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.math3.stat.correlation.*;
import org.apache.commons.math3.stat.ranking.*;

public class Main {

	public static void main(String[] args) throws NumberFormatException,
			IOException {

		/** -inputPath -outputPath */
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		String line;

		long numNodes = 0;
		long numEdges = 0;
		long srcID;
		long destID;
		int edgeCounter = 0;

		HashMap < Long,long[]> edges = new HashMap<Long, long[]>();
		
		HashMap<Long,Double> nodeDegrees = new HashMap<Long,Double>();
		
		double[] dataX= new double[0];
		double[] dataY= new double[0];

		while ((line = br.readLine()) != null) {
			if (line.startsWith("# Nodes:")) {

				line = line.substring(9, line.length());
				numNodes = Long.parseLong(line.substring(0, line.indexOf(" ")));

				line = line.substring(line.lastIndexOf(" ") + 1, line.length());
				numEdges = Long.parseLong(line) / 2;

				
			} else if (!line.startsWith("#")) {
				/*if (numEdges == 0 || numNodes == 0) {
					System.out.println("Number of edges or nodes is 0");
					System.exit(1);
				}*/
				line = line.replace("\t", " ");
				srcID = Long.parseLong(line.substring(0, line.indexOf(" ")));
				destID = Long.parseLong(line.substring(line.indexOf(" ") + 1,
						line.length()));

				if (srcID <= destID) {
					long[] edge=new long[2];
					edge[0]=srcID;
					edge[1]=destID;
					edges.put(new Long(edgeCounter),edge);

					
					if(!nodeDegrees.containsKey(srcID)){
						nodeDegrees.put(srcID, new Double(1));
					}else{
						nodeDegrees.put(srcID, nodeDegrees.get(srcID)+1);
					}
					
					
					if(!nodeDegrees.containsKey(destID)){
						nodeDegrees.put(destID, new Double(1));
					}else{
						nodeDegrees.put(destID, nodeDegrees.get(destID)+1);
					}
					
					edgeCounter++;
					if(edgeCounter%1000==0){
						System.out.println("Line "+ edgeCounter +",  edge:"+ line);
					}
				}
			}
		}
		
		br.close();
		dataX=new double[edgeCounter];
		dataY=new double[edgeCounter];
		for(Entry<Long, long[]> entry: edges.entrySet()){
			long[] edge = entry.getValue();
			if(Math.random()>(1/2d)){
				dataX[entry.getKey().intValue()] = nodeDegrees.get(edge[0])+Math.random();
				dataY[entry.getKey().intValue()] = nodeDegrees.get(edge[1])+Math.random();
			}else{
				dataX[entry.getKey().intValue()] = nodeDegrees.get(edge[1])+Math.random();
				dataY[entry.getKey().intValue()] = nodeDegrees.get(edge[0])+Math.random();
			}
		}
		
		RankingAlgorithm natural=new NaturalRanking(NaNStrategy.MINIMAL, TiesStrategy.SEQUENTIAL );
		SpearmansCorrelation spearmansCor = new SpearmansCorrelation( natural);
		double spearmanRank= spearmansCor.correlation(dataX, dataY);

		FileWriter output = new FileWriter(args[1], true);
		output.write("\n"+args[0]+" Spearmans rho= "+spearmanRank +" Edges="+numEdges+" Nodes= "+numNodes);
		output.flush();
		output.close();
		
	}

}
