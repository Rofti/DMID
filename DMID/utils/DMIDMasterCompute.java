package org.apache.giraph.examples.utils;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVectorSumAggregator;
import org.apache.giraph.aggregators.matrix.sparse.DoubleSparseVector;
import org.apache.giraph.aggregators.matrix.sparse.DoubleSparseVectorSumAggregator;
import org.apache.giraph.examples.DMIDComputation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Master compute associated with {@link DMIDComputation}. It registers required
 * aggregators.
 */
public class DMIDMasterCompute extends DefaultMasterCompute {

	public static final String RESTART_COUNTER_AGG = "aggRestart";
	public static final double PROFTIABILITY_DELTA = 0.1;
	private static final boolean LOG_AGGS = true;

	@Override
	public void initialize() throws InstantiationException,
			IllegalAccessException {

		registerAggregator(DMIDComputation.DA_AGG,
				DoubleDenseVectorSumAggregator.class);
		registerPersistentAggregator(DMIDComputation.LS_AGG,
				DoubleDenseVectorSumAggregator.class);
		registerPersistentAggregator(DMIDComputation.FD_AGG,
				DoubleSparseVectorSumAggregator.class);
		registerPersistentAggregator(DMIDComputation.GL_AGG,
				DoubleSparseVectorSumAggregator.class);

		registerAggregator(DMIDComputation.NEW_MEMBER_AGG,
				BooleanOrAggregator.class);
		registerAggregator(DMIDComputation.NOT_ALL_ASSIGNED_AGG,
				BooleanOrAggregator.class);

		registerPersistentAggregator(DMIDComputation.ITERATION_AGG,
				LongMaxAggregator.class);

		registerPersistentAggregator(DMIDComputation.PROFITABILITY_AGG,
				DoubleMaxAggregator.class);
		registerPersistentAggregator(RESTART_COUNTER_AGG,
				LongMaxAggregator.class);
		registerAggregator(DMIDComputation.RW_INFINITYNORM_AGG,
				DoubleMaxAggregator.class);
		registerAggregator(DMIDComputation.RW_FINISHED_AGG,
				LongMaxAggregator.class);

		setAggregatedValue(DMIDComputation.PROFITABILITY_AGG,
				new DoubleWritable(0.9));
		setAggregatedValue(RESTART_COUNTER_AGG, new LongWritable(1));
		setAggregatedValue(DMIDComputation.ITERATION_AGG, new LongWritable(0));

	}

	@Override
	public void compute() {
		/**
		 * setAggregatorValue sets the value for the aggregator after master
		 * compute, before starting vertex compute of the same superstep. Does
		 * not work with OverwriteAggregators
		 */

		DoubleWritable norm = getAggregatedValue(DMIDComputation.RW_INFINITYNORM_AGG);

		if (getSuperstep() > 3
				&& (norm.get() <= 0.001 || getSuperstep() > DMIDComputation.RW_ITERATIONBOUND + 3)) {
			setAggregatedValue(DMIDComputation.RW_FINISHED_AGG,
					new LongWritable(getSuperstep()));
		}

		LongWritable iterCount = getAggregatedValue(DMIDComputation.ITERATION_AGG);
		LongWritable rwFinished = getAggregatedValue(DMIDComputation.RW_FINISHED_AGG);
		boolean hasCascadingStarted = false;
		LongWritable newIterCount = new LongWritable((iterCount.get() + 1));

		if (iterCount.get() != 0) {
			/** Cascading behavior started increment the iteration count */
			setAggregatedValue(DMIDComputation.ITERATION_AGG, newIterCount);
			hasCascadingStarted = true;
		}

		if (getSuperstep() == rwFinished.get() + 4) {
			setAggregatedValue(DMIDComputation.ITERATION_AGG, new LongWritable(
					1));
			hasCascadingStarted = true;
			initializeGL();
		}

		if (hasCascadingStarted && (newIterCount.get() % 3 == 1)) {
			/** first step of one iteration */

			BooleanWritable newMember = getAggregatedValue(DMIDComputation.NEW_MEMBER_AGG);
			BooleanWritable notAllAssigned = getAggregatedValue(DMIDComputation.NOT_ALL_ASSIGNED_AGG);

			if ((notAllAssigned.get() == true) && (newMember.get() == false)) {
				/**
				 * RESTART Cascading Behavior with lower profitability threshold
				 */

				long restartCount = getAggregatedValue(RESTART_COUNTER_AGG);
				double newThreshold = 1 - (PROFTIABILITY_DELTA * (restartCount + 1));

				setAggregatedValue(RESTART_COUNTER_AGG, new LongWritable(
						restartCount + 1));
				setAggregatedValue(DMIDComputation.PROFITABILITY_AGG,
						new DoubleWritable(newThreshold));
				setAggregatedValue(DMIDComputation.ITERATION_AGG,
						new LongWritable(1));

			}

		}

		if (hasCascadingStarted && (iterCount.get() % 3 == 2)) {
			/** Second step of one iteration */
			/**
			 * Set newMember aggregator and notAllAssigned aggregator back to
			 * initial value
			 */

			setAggregatedValue(DMIDComputation.NEW_MEMBER_AGG,
					new BooleanWritable(false));
			setAggregatedValue(DMIDComputation.NOT_ALL_ASSIGNED_AGG,
					new BooleanWritable(false));
		}

		if (LOG_AGGS) {
			if (getSuperstep() == DMIDComputation.RW_ITERATIONBOUND + 4) {
				DoubleDenseVector convergedDA = getAggregatedValue(DMIDComputation.DA_AGG);
				System.out.print("Aggregator DA after convergence: \nsize="
						+ getTotalNumVertices() + "\n[ ");
				for (int i = 0; i < getTotalNumVertices(); ++i) {
					System.out.print(convergedDA.get(i));
					if (i != getTotalNumVertices() - 1) {
						System.out.print(" , ");
					} else {
						System.out.println(" ]\n");
					}
				}
			}
			if (getSuperstep() == DMIDComputation.RW_ITERATIONBOUND + 6) {
				DoubleDenseVector leadershipVector = getAggregatedValue(DMIDComputation.LS_AGG);
				System.out.print("Aggregator LS: \nsize="
						+ getTotalNumVertices() + "\n[ ");
				for (int i = 0; i < getTotalNumVertices(); ++i) {
					System.out.print(leadershipVector.get(i));
					if (i != getTotalNumVertices() - 1) {
						System.out.print(" , ");
					} else {
						System.out.println(" ]\n");
					}
				}
			}
		}
	}

	/**
	 * Initilizes the global leader aggregator with 1 for every vertex with a
	 * higher number of followers than the average.
	 */
	private void initializeGL() {
		DoubleSparseVector initGL = new DoubleSparseVector(
				(int) getTotalNumVertices());
		DoubleSparseVector vecFD = getAggregatedValue(DMIDComputation.FD_AGG);

		double averageFD = 0.0;
		int numLocalLeader = 0;
		/** get averageFollower degree */
		for (int i = 0; i < getTotalNumVertices(); ++i) {
			averageFD += vecFD.get(i);
			if (vecFD.get(i) != 0) {
				numLocalLeader++;
			}
		}
		if (numLocalLeader != 0) {
			averageFD = (double) averageFD / numLocalLeader;
		}
		/** set flag for globalLeader */
		if (LOG_AGGS) {
			System.out.print("Global Leader:");
		}
		for (int i = 0; i < getTotalNumVertices(); ++i) {
			if (vecFD.get(i) > averageFD) {
				initGL.set(i, 1.0);
				if (LOG_AGGS) {
					System.out.print("  " + i + "  ");
				}
			}
		}
		if (LOG_AGGS) {
			System.out.println("\n");
		}
		/** set Global Leader aggregator */
		setAggregatedValue(DMIDComputation.GL_AGG, initGL);

		/** set not all vertices assigned aggregator to true */
		setAggregatedValue(DMIDComputation.NOT_ALL_ASSIGNED_AGG,
				new BooleanWritable(true));

	}
}
