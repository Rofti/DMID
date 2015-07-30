package org.apache.giraph.examples.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.giraph.aggregators.matrix.sparse.DoubleSparseVector;

import org.apache.hadoop.io.Writable;

/**
 * Vertex value used for the DMID algorithm
 */
public class DMIDVertexValue implements Writable {
	/** Invalid weightedInDegree */
	public static final Double INVALID_DEGREE = Double.valueOf(-1);
	/**
	 * Stores the sum of all incoming edges
	 */
	private Double weightedInDegree;
	/**
	 * Stores the Membership value for each community. The Long value denotes
	 * the ID of the community leader. The Double value determines the
	 * membership degree
	 */
	private HashMap<Long, Double> membershipDegree;
	/**Only used in the binary search variant of DMID.
	 * Stores the best valid cover found in the cascading phase.
	 * */
	private HashMap<Long, Double> bestValidMemDeg;
	/**
	 * Column of the disassortativity matrix AS with index vertex.getID() 
	 */
	private DoubleSparseVector disCol;
	/**
	 * Size of disCol
	 */
	private Long disSize; 

	/**
	 * Default constructor
	 */
	public DMIDVertexValue() {
		this(new Double(INVALID_DEGREE), new HashMap<Long, Double>(), new DoubleSparseVector(), new Long(0));
	}

	/**
	 * Parametrized constructor
	 *
	 * @param weightedInDegree
	 *            sum of all incoming edges
	 * @param MembershipDegree
	 *            Map with community leader ID as a key and membership degree as
	 *            value
	 * @param disCol 
	 *            Column of the disassortativity matrix AS with index vertex.getID()
	 * @param disSize
	 *            Size of disCol         
	 */
	public DMIDVertexValue(Double weightedInDegree,
			HashMap<Long, Double> MembershipDegree, DoubleSparseVector disCol, Long disSize) {
		this.weightedInDegree = weightedInDegree;
		this.membershipDegree = MembershipDegree;
		this.bestValidMemDeg=new HashMap<Long,Double>();
		this.disCol = disCol;
		this.disSize = disSize;
	}

	@Override
	public void readFields(DataInput input) throws IOException {

		this.weightedInDegree = input.readDouble();

		/**
		 * Size of the MembershipDegree map.
		 */
		int memSize;
		memSize = input.readInt();

		Long leaderID;
		Double memDegree;
		this.membershipDegree = new HashMap<Long, Double>();

		for (int i = 0; i < memSize; ++i) {
			leaderID = input.readLong();
			memDegree = input.readDouble();

			this.membershipDegree.put(leaderID, memDegree);
		}
		
		memSize = input.readInt();
		this.bestValidMemDeg = new HashMap<Long, Double>();

		for (int i = 0; i < memSize; ++i) {
			leaderID = input.readLong();
			memDegree = input.readDouble();

			this.bestValidMemDeg.put(leaderID, memDegree);
		}
		
		/**
		 * Size of the disassortativity vector disCol.
		 */
		
		this.disSize = input.readLong();
		
		for(int i = 0; i < disSize; ++i) {
			this.disCol.set(i, input.readDouble());
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {

		output.writeDouble(this.weightedInDegree);

		int memSize = this.membershipDegree.size();
		output.writeInt(memSize);

		for (Map.Entry<Long, Double> entry : this.membershipDegree.entrySet()) {
			output.writeLong(entry.getKey());
			output.writeDouble(entry.getValue());
		}
		
		memSize = this.bestValidMemDeg.size();
		output.writeInt(memSize);
		
		for (Map.Entry<Long, Double> entry : this.bestValidMemDeg.entrySet()) {
			output.writeLong(entry.getKey());
			output.writeDouble(entry.getValue());
		}
		
		output.writeLong(this.disSize);
		
		for (int i=0; i < this.disSize; ++i) {
			output.writeDouble(this.disCol.get(i));
		}
	}

	public Double getWeightedInDegree() {
		return weightedInDegree;
	}

	public void setWeightedInDegree(Double weightedInDegree) {
		this.weightedInDegree = weightedInDegree;
	}

	public HashMap<Long, Double> getMembershipDegree() {
		return membershipDegree;
	}

	public void setBestValidMemDeg(HashMap<Long, Double> bestValidMemDeg) {
		this.bestValidMemDeg = bestValidMemDeg;
	}
	
	public HashMap<Long, Double> getBestValidMemDeg() {
		return bestValidMemDeg;
	}

	public void setMembershipDegree(HashMap<Long, Double> membershipDegree) {
		this.membershipDegree = membershipDegree;
	}
	
	public Long getDisSize() {
		return this.disSize;
	}

	public void setDisSize(Long disSize) {
		this.disSize = disSize;
	}
	
	public DoubleSparseVector getDisCol() {
		return this.disCol;
	}

	public void setDisCol(DoubleSparseVector disCol, Long disSize) {
		this.disCol = disCol;
		this.disSize = disSize;
	}
}
