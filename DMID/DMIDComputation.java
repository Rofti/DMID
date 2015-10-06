package org.apache.giraph.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.utils.LongDoubleMessage;
import org.apache.giraph.examples.utils.DMIDVertexValue;
import org.apache.giraph.aggregators.matrix.sparse.DoubleSparseVector;
import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Demonstrates the basic Giraph DMID implementation. DMID stands for
 * Disassortativity Degree Mixing and Information Diffusion.
 */
@Algorithm(name = "DMID abstract")
public abstract class DMIDComputation
		extends
		BasicComputation<LongWritable, DMIDVertexValue, DoubleWritable, LongDoubleMessage> {

	/** Aggregator name of the DMID disassortativity vector DA */
	public static final String DA_AGG = "aggDA";

	/** Aggregator name of the DMID leadership vector LS */
	public static final String LS_AGG = "aggLS";

	/**
	 * Aggregator name of the FollowerDegree vector where entry i determines how
	 * many follower vertex i has
	 */
	public static final String FD_AGG = "aggFD";

	/**
	 * Aggregator name of the DMID GlobalLeader vector where entry i determines
	 * if vertex i is a global leader
	 */
	public static final String GL_AGG = "aggGL";

	/**
	 * Aggregator name of the new Member flag Indicates if a vertex adopted a
	 * behavior in the Cascading Behavior Phase of DMID
	 **/
	public static final String NEW_MEMBER_AGG = "aggNewMember";

	/**
	 * Aggregator name of the all vertices assigned flag Indicates if there is a
	 * vertex that did not adopted a behavior in the Cascading Behavior Phase of
	 * DMID
	 **/
	public static final String NOT_ALL_ASSIGNED_AGG = "aggNotAllAssigned";

	/**
	 * Aggregator name of the iteration count. Denotes the current iteration of
	 * the cascading behavior phase times 3 (each step in the cascading behavior
	 * phase is divided into 3 supersteps)
	 */
	public static final String ITERATION_AGG = "aggIT";

	/**
	 * Aggregator name for the profitability threshold of the cascading behavior
	 * phase of DMID
	 */
	public static final String PROFITABILITY_AGG = "aggProfit";

	/** Maximum steps for the random walk, corresponds to t*. Default = 1000 */
	public static final long RW_ITERATIONBOUND = 10;

	/**
	 * Aggregator name for the random walk precision factor. Stores the infinity
	 * norm of the difference between the updated vector and the previous one.
	 * The random walk phase ends when the aggregator stores a value smaller
	 * than 0.001.
	 */
	//public static final String RW_INFINITYNORM_AGG = "aggPrecision";
	
	/** Aggregator name. Holds the superstep on which the random walk phase finished*/
	//public static final String RW_FINISHED_AGG="aggFinishedRW";

	@Override
	public void compute(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) throws IOException {

		if (getSuperstep() == 0) {
			superstep0(vertex, messages);
		}

		if (getSuperstep() == 1) {
			superstep1(vertex, messages);
		}

		if (getSuperstep() == 2) {
			superstep2(vertex, messages);
		}
		
		//double rwPrecision = ((DoubleWritable)getAggregatedValue(RW_INFINITYNORM_AGG)).get();
		
		if ((getSuperstep() >= 3 && getSuperstep() <= RW_ITERATIONBOUND + 3) /*&& (getSuperstep() == 3 || rwPrecision >0.001)*/ ) {
			/**
			 * TODO: Integrate a precision factor for the random walk phase. The
			 * phase ends when the infinity norm of the difference between the
			 * updated vector and the previous one is smaller than this factor.
			 */
			superstepRW(vertex, messages);
		}

		long rwFinished = RW_ITERATIONBOUND + 4;
		
		
		if (getSuperstep() == rwFinished) {
			superstep4(vertex, messages);
		}

		if (getSuperstep() == rwFinished +1) {
			/**
			 * Superstep 0 and RW_ITERATIONBOUND + 5 are identical. Therefore
			 * call superstep0
			 */
			superstep0(vertex, messages);
		}

		if (getSuperstep() == rwFinished+2) {
			superstep6(vertex, messages);
		}

		if (getSuperstep() == rwFinished + 3) {
			superstep7(vertex, messages);
			
		}

		LongWritable iterationCounter = getAggregatedValue(ITERATION_AGG);
		double it = iterationCounter.get();

		if (getSuperstep() >= rwFinished +4
				&& (it % 3 == 1 )) {
			superstep8(vertex, messages);
		}
		if (getSuperstep() >= rwFinished +5
				&& (it % 3 == 2 )) {
			superstep9(vertex, messages);
		}
		if (getSuperstep() >= rwFinished +6
				&& (it % 3 == 0 )) {
			superstep10(vertex, messages);
		}

	}

	/**
	 * SUPERSTEP 0: send a message along all outgoing edges. Message contains
	 * own VertexID and the edge weight.
	 */
	private void superstep0(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		long vertexID = vertex.getId().get();

		for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
			LongDoubleMessage msg = new LongDoubleMessage(vertexID, edge
					.getValue().get());
			sendMessage(edge.getTargetVertexId(), msg);
		}
	}

	/**
	 * SUPERSTEP 1: Calculate and save new weightedInDegree. Send a message of
	 * the form (ID,weightedInDegree) along all incoming edges (send every node
	 * a reply)
	 */
	private void superstep1(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		double weightedInDegree = 0.0;

		/** vertices that need a reply containing this vertexs weighted indegree */
		HashSet<Long> predecessors = new HashSet<Long>();

		for (LongDoubleMessage msg : messages) {
			/**
			 * sum of all incoming edge weights (weightedInDegree).
			 * msg.getValue() contains the edgeWeight of an incoming edge. msg
			 * was send by msg.getSourceVertexId()
			 * 
			 */
			predecessors.add(msg.sourceVertexId);
			weightedInDegree += msg.getValue();
		}
		/** update new weightedInDegree */
		DMIDVertexValue vertexValue = vertex.getValue();
		vertexValue.setWeightedInDegree(weightedInDegree);
		vertex.setValue(vertexValue);

		LongDoubleMessage msg = new LongDoubleMessage(vertex.getId().get(),
				weightedInDegree);
		for (Long msgTargetID : predecessors) {
			sendMessage(new LongWritable(msgTargetID), msg);
		}
	}

	/**
	 * SUPERSTEP 2: Iterate over all messages. Set the entries of the
	 * disassortativity matrix column with index vertexID. Normalize the column.
	 * Save the column as a part of the vertexValue. Aggregate DA with value 1/N
	 * to initialize the Random Walk.
	 */
	private void superstep2(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		/** Weight= weightedInDegree */
		double senderWeight = 0.0;
		double ownWeight = vertex.getValue().getWeightedInDegree();
		long senderID;

		/** disValue = disassortativity value of senderID and ownID */
		double disValue = 0;
		DoubleSparseVector disVector = new DoubleSparseVector(
				(int) getTotalNumVertices());

		/** Sum of all disVector entries */
		double disSum = 0;

		/** Set up new disCol */
		for (LongDoubleMessage msg : messages) {

			senderID = msg.getSourceVertexId();
			senderWeight = msg.getValue();

			disValue = Math.abs(ownWeight - senderWeight);
			disSum += disValue;

			disVector.set((int) senderID, disValue);
		}
		/**
		 * Normalize the new disCol (Note: a new Vector is automatically
		 * initialized 0.0f entries)
		 */
		
		for (int i = 0; disSum != 0 && i < (int) getTotalNumVertices(); ++i) {
			disVector.set(i, (disVector.get(i) / disSum));
			
		}

		/** save the new disCol in the vertexValue */
		vertex.getValue().setDisCol(disVector, getTotalNumVertices());
		/**
		 * Initialize DA for the RW steps with 1/N for your own entry
		 * (aggregatedValue will be(1/N,..,1/N) in the next superstep)
		 * */
		DoubleDenseVector init = new DoubleDenseVector(
				(int) getTotalNumVertices());
		init.set((int) vertex.getId().get(), (double) 1.0
				/ getTotalNumVertices());

		aggregate(DA_AGG, init);
	}

	/**
	 * SUPERSTEP 3 - RW_ITERATIONBOUND+3: Calculate entry DA^(t+1)_ownID using
	 * DA^t and disCol. Save entry in the DA aggregator.
	 */
	private void superstepRW(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		DoubleDenseVector curDA = getAggregatedValue(DA_AGG);
		DoubleSparseVector disCol = vertex.getValue().getDisCol();

		/**
		 * Calculate DA^(t+1)_ownID by multiplying DA^t (=curDA) and column
		 * vertexID of T (=disCol)
		 */
		/** (corresponds to vector matrix multiplication R^1xN * R^NxN) */
		double newEntryDA = 0.0;
		for (int i = 0; i < getTotalNumVertices(); ++i) {
			newEntryDA += (curDA.get(i) * disCol.get(i));
		}
		
		DoubleDenseVector newDA = new DoubleDenseVector(
				(int) getTotalNumVertices());
		newDA.set((int) vertex.getId().get(), newEntryDA);
		aggregate(DA_AGG, newDA);
		
	}

	/**
	 * SUPERSTEP RW_ITERATIONBOUND+4: Calculate entry LS_ownID using DA^t* and
	 * weightedInDegree. Save entry in the LS aggregator.
	 */
	private void superstep4(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		DoubleDenseVector finalDA = getAggregatedValue(DA_AGG);
		double weightedInDegree = vertex.getValue().getWeightedInDegree();
		int vertexID = (int) vertex.getId().get();

		DoubleDenseVector tmpLS = new DoubleDenseVector(
				(int) getTotalNumVertices());
		tmpLS.set(vertexID, (weightedInDegree * finalDA.get(vertexID)));

		aggregate(LS_AGG, tmpLS);
	}

	/**
	 * SUPERSTEP RW_IT+6: iterate over received messages. Determine if this
	 * vertex has more influence on the sender than the sender has on this
	 * vertex. If that is the case the sender is a possible follower of this
	 * vertex and therefore vertex sends a message back containing the influence
	 * value on the sender. The influence v-i has on v-j is (LS-i * w-ji) where
	 * w-ji is the weight of the edge from v-j to v-i.
	 * */
	private void superstep6(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		/** Weight= weightedInDegree */
		double senderWeight = 0.0;
		long senderID;

		boolean hasEdgeToSender = false;

		for (LongDoubleMessage msg : messages) {

			senderID = msg.getSourceVertexId();
			senderWeight = msg.getValue();

			DoubleDenseVector vecLS = getAggregatedValue(LS_AGG);

			/**
			 * hasEdgeToSender determines if sender has influence on this vertex
			 */
			hasEdgeToSender = false;
			for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
				if (edge.getTargetVertexId().get() == senderID) {

					hasEdgeToSender = true;
					/**
					 * Has this vertex more influence on the sender than the
					 * sender on this vertex?
					 */
					if (senderWeight * vecLS.get((int) vertex.getId().get()) > edge
							.getValue().get() * vecLS.get((int) senderID)) {
						/** send new message */
						LongDoubleMessage newMsg = new LongDoubleMessage(vertex
								.getId().get(), senderWeight
								* vecLS.get((int) vertex.getId().get()));

						sendMessage(new LongWritable(senderID), newMsg);
					}
				}
			}
			if (!hasEdgeToSender) {
				/** send new message */
				LongDoubleMessage newMsg = new LongDoubleMessage(vertex.getId()
						.get(), senderWeight
						* vecLS.get((int) vertex.getId().get()));

				sendMessage(new LongWritable(senderID), newMsg);
			}

		}
	}

	/**
	 * SUPERSTEP RW_IT+7: Find the local leader of this vertex. The local leader
	 * is the sender of the message with the highest influence on this vertex.
	 * There may be more then one local leader. Add 1/k to the FollowerDegree
	 * (aggregator) of the k local leaders found.
	 **/
	private void superstep7(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		/** maximum influence on this vertex */
		double maxInfValue = 0;

		/** Set of possible local leader for this vertex. Contains VertexID's */
		HashSet<Long> leaderSet = new HashSet<Long>();

		/** Find possible local leader */
		for (LongDoubleMessage msg : messages) {

			if (msg.getValue() >= maxInfValue) {
				if (msg.getValue() > maxInfValue) {
					/** new distinct leader found. Clear set */
					leaderSet.clear();

				}
				/**
				 * has at least the same influence as the other possible leader.
				 * Add to set
				 */
				leaderSet.add(msg.getSourceVertexId());

			}
		}

		int leaderSetSize = leaderSet.size();
		DoubleSparseVector newFD = new DoubleSparseVector(
				(int) getTotalNumVertices());

		for (Long leaderID : leaderSet) {
			newFD.set(leaderID.intValue(), (double) 1.0 / leaderSetSize);

		}

		aggregate(FD_AGG, newFD);
	}

	/**
	 * SUPERSTEP RW_IT+8: Startpoint and first iteration point of the cascading
	 * behavior phase.
	 **/

	void superstep8(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		Long vertexID = vertex.getId().get();
		DoubleWritable profitability = getAggregatedValue(DMIDComputation.PROFITABILITY_AGG);
		/** Is this vertex a global leader? Global Leader do not change behavior */
		if (!vertex.getValue().getMembershipDegree().containsKey(vertexID)||profitability.get()<0) {
			BooleanWritable notAllAssigned = getAggregatedValue(NOT_ALL_ASSIGNED_AGG);
			BooleanWritable newMember = getAggregatedValue(NEW_MEMBER_AGG);
			if (notAllAssigned.get()) {
				/** There are vertices that are not part of any community */

				if (!newMember.get()) {
					/**
					 * There are no changes in the behavior cascade but not all
					 * vertices are assigned
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
						 * message of the form (ownID, community ID of interest)
						 */
						if(vertex.getValue()
								.getMembershipDegree().get(leaderID)==0){
							LongDoubleMessage msg = new LongDoubleMessage(vertexID,
									leaderID);
							sendMessageToAllEdges(vertex, msg);
						}

					}
				} else {
					vertex.voteToHalt();
				}
			} else {

				/** All vertices are assigned to at least one community */
				/** TERMINATION */
				vertex.voteToHalt();
			}
		} else {
			vertex.voteToHalt();
		}
	}

	/**
	 * SUPERSTEP RW_IT+9: Second iteration point of the cascading behavior
	 * phase.
	 **/
	private void superstep9(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages) {

		/**
		 * iterate over the requests to send this vertex behavior to these
		 * specific communities
		 */
		for (LongDoubleMessage msg : messages) {

			long leaderID = ((long) msg.getValue());
			/**
			 * send a message back with the same double entry if this vertex is
			 * part of this specific community
			 */
			
			if (vertex.getValue().getMembershipDegree().get(leaderID) != 0.0) {
				LongDoubleMessage answerMsg = new LongDoubleMessage(vertex
						.getId().get(), leaderID);
				sendMessage(new LongWritable(msg.getSourceVertexId()),
						answerMsg);
			}
		}
	}

	/**
	 * SUPERSTEP RW_IT+10: Third iteration point of the cascading behavior
	 * phase.
	 **/
	abstract void superstep10(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex,
			Iterable<LongDoubleMessage> messages); 

	/**
	 * Initialize the MembershipDegree vector.
	 **/
	private void initilaizeMemDeg(
			Vertex<LongWritable, DMIDVertexValue, DoubleWritable> vertex) {

		DoubleSparseVector vecGL = getAggregatedValue(GL_AGG);
		HashMap<Long, Double> newMemDeg = new HashMap<Long, Double>();

		for (long i = 0; i < getTotalNumVertices(); ++i) {
			/** only global leader have entries 1.0 the rest will return 0*/
			if(vecGL.get((int) i)!=0){
				/** is entry i a global leader?*/
				if(i == vertex.getId().get()){
					/**
					 * This vertex is a global leader. Set Membership degree to
					 * 100%
					 */
					newMemDeg.put(new Long(i), new Double(1.0));

				}
				else{
					newMemDeg.put(new Long(i), new Double(0.0));

				}
				
			}
		}
		/** is entry i a global leader? */
		if (vecGL.get((int) vertex.getId().get())!=0 ) {
			/**
			 * This vertex is a global leader. Set Membership degree to
			 * 100%
			 */
			newMemDeg.put(new Long(vertex.getId().get()), new Double(1.0));
		}
		
		vertex.getValue().setMembershipDegree(newMemDeg);
	}
}
