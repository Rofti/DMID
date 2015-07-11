package org.apache.giraph.examples;

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.aggregators.matrix.sparse.DoubleSparseVector;
import org.apache.giraph.examples.utils.BinarySearchDMIDMasterCompute;
import org.apache.giraph.examples.utils.DMIDVertexValue;
import org.apache.giraph.examples.utils.LongDoubleMessage;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class BinarySearchDMIDComputation extends DMIDComputation {

	/** maximum number of binary search cascading steps */
	private static final int CASCADINGBOUND = 10;

	/**
	 * SUPERSTEP RW_IT+8: Startpoint and first iteration point of the cascading
	 * behavior phase.
	 **/
	@Override
	public void superstep8(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		Long vertexID = vertex.getId().get();

		/** Is this vertex a global leader? Global Leader do not change behavior */
		if (!vertex.getValue().getMembershipDegree().containsKey(vertexID)) {

			long numRestarts = getAggregatedValue(BinarySearchDMIDMasterCompute.RESTART_COUNTER_AGG);
			if (numRestarts >= CASCADINGBOUND) {
				/** put best valid cover/solution as MembershipDegree */
				vertex.getValue().setMembershipDegree(
						vertex.getValue().getBestValidMemDeg());
				vertex.voteToHalt();
			} else {
				BooleanWritable notAllAssigned = getAggregatedValue(NOT_ALL_ASSIGNED_AGG);
				if (notAllAssigned.get()) {
					/** There are vertices that are not part of any community */
					BooleanWritable newMember = getAggregatedValue(NEW_MEMBER_AGG);
					if (!newMember.get()) {
						/**
						 * There are no changes in the behavior cascade but not
						 * all vertices are assigned
						 */
						/** RESTART */
						/** set MemDeg back to initial value */
						initilaizeMemDeg(vertex);
					}
					/** ANOTHER ROUND */
					/**
					 * every 0 entry means vertex is not part of this community
					 * request all successors to send their behavior to these
					 * specific communities.
					 **/

					/** In case of first init test again if vertex is leader */
					if (!vertex.getValue().getMembershipDegree()
							.containsKey(vertexID)) {
						for (Long leaderID : vertex.getValue()
								.getMembershipDegree().keySet()) {
							/**
							 * message of the form (ownID, community ID of
							 * interest)
							 */
							LongDoubleMessage msg = new LongDoubleMessage(
									vertexID, leaderID);
							sendMessageToAllEdges(vertex, msg);
						}
					} else {
						vertex.voteToHalt();
					}
				} else {

					/** All vertices are assigned to at least one community */
					/** TERMINATION of one cascade */

					/** save best valid cover/solution as BestValidMemDeg */
					vertex.getValue().setBestValidMemDeg(
							vertex.getValue().getMembershipDegree());

					/** RESTART */
					/** set MemDeg back to initial value */
					initilaizeMemDeg(vertex);

					/** ANOTHER Cascade */
					if (!vertex.getValue().getMembershipDegree()
							.containsKey(vertexID)) {
						for (Long leaderID : vertex.getValue()
								.getMembershipDegree().keySet()) {
							/**
							 * message of the form (ownID, community ID of
							 * interest)
							 */
							LongDoubleMessage msg = new LongDoubleMessage(
									vertexID, leaderID);
							sendMessageToAllEdges(vertex, msg);
						}
					} else {
						vertex.voteToHalt();
					}
				}
			}
		} else {
			vertex.voteToHalt();
		}
	}

	private void initilaizeMemDeg(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex) {

		DoubleSparseVector vecGL = getAggregatedValue(GL_AGG);
		HashMap<Long, Double> newMemDeg = new HashMap<Long, Double>();

		for (long i = 0; i < getTotalNumVertices(); ++i) {
			/** is entry i a global leader? */
			if (vecGL.get((int) i) == 1.0) {

				if (((long) i) == vertex.getId().get()) {
					/**
					 * This vertex is a global leader. Set Membership degree to
					 * 100%
					 */
					newMemDeg.put(new Long(i), new Double(1.0));
				} else {
					newMemDeg.put(new Long(i), new Double(0.0));
				}
			}
		}

		vertex.getValue().setMembershipDegree(newMemDeg);
	}

	/** The same as the superstep10 of the basic DMID */
	@Override
	public void superstep10(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		long vertexID = vertex.getId().get();

		/** Is this vertex a global leader? */
		if (!vertex.getValue().getMembershipDegree().containsKey(vertexID)) {
			/** counts per communities the number of successors which are member */
			HashMap<Long, Double> membershipCounter = new HashMap<Long, Double>();
			double previousCount = 0.0;

			for (LongDoubleMessage msg : messages) {
				/**
				 * the msg value is the index of the community the sender is a
				 * member of
				 */
				Long leaderID = ((long) msg.getValue());

				if (membershipCounter.containsKey(leaderID)) {
					/** increase count by 1 */
					previousCount = membershipCounter.get(leaderID);
					membershipCounter.put(leaderID, previousCount + 1);

				} else {
					membershipCounter.put(leaderID, 1.0);
				}
			}
			/** profitability threshold */
			DoubleWritable threshold = getAggregatedValue(PROFITABILITY_AGG);

			LongWritable iterationCounter = getAggregatedValue(ITERATION_AGG);

			for (Map.Entry<Long, Double> entry : membershipCounter.entrySet()) {
				if ((entry.getValue() / vertex.getNumEdges()) > threshold.get()) {
					/** its profitable to become a member, set value */
					vertex.getValue()
							.getMembershipDegree()
							.put(entry.getKey(),
									(1.0 / Math.pow(iterationCounter.get() / 3,
											2)));
					aggregate(NEW_MEMBER_AGG, new BooleanWritable(true));
				}
			}
			boolean isPartOfAnyCommunity = false;
			for (Map.Entry<Long, Double> entry : vertex.getValue()
					.getMembershipDegree().entrySet()) {
				if (entry.getValue() != 0.0) {
					isPartOfAnyCommunity = true;
				}
			}
			if (!isPartOfAnyCommunity) {
				aggregate(NOT_ALL_ASSIGNED_AGG, new BooleanWritable(true));
			}
		}

	}

}
