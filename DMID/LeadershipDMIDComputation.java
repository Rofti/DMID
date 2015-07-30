package org.apache.giraph.examples;

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import org.apache.giraph.examples.utils.DMIDMasterCompute;
import org.apache.giraph.examples.utils.DMIDVertexValue;
import org.apache.giraph.examples.utils.LongDoubleMessage;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Implements the leadership variant of DMID. Differs from the basic
 * implementation only in the cascading behavior. Profitability depends on
 * leadership values and is not uniform.
 * */
@Algorithm(name = "DMID leadership variant")
public class LeadershipDMIDComputation extends DMIDComputation {
	/**
	 * SUPERSTEP RW_IT+10: Third iteration point of the cascading behavior
	 * phase.
	 **/
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

			DoubleDenseVector vecLS = getAggregatedValue(LS_AGG);
			LongWritable numCascadings = getAggregatedValue(DMIDMasterCompute.RESTART_COUNTER_AGG);
			/** profitability threshold */
			double threshold = vecLS.get((int) vertex.getId().get())
					- (numCascadings.get() * DMIDMasterCompute.PROFTIABILITY_DELTA);

			LongWritable iterationCounter = getAggregatedValue(ITERATION_AGG);

			for (Map.Entry<Long, Double> entry : membershipCounter.entrySet()) {

				if ((entry.getValue() / vertex.getNumEdges()) > threshold) {
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
