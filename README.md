# DMID
Implementation of the overlapping community detection algorithm DMID for giraph as part of the bachelor thesis "Pregel: Parallel Implementation of Overlapping Community Detection Algorithms" at the chair of Information Systems RWTH Aachen University.


Command:
$HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples/target/giraph-examples-1.1.0-for-hadoop-1.2.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.DMIDComputation -vif org.apache.giraph.examples.io.formats.DMIDVertexInputFormat -vip /input/graphOutput.txt -vof org.apache.giraph.examples.io.formats.DMIDVertexOutputFormat -op /output -w 1 -mc org.apache.giraph.examples.utils.DMIDMasterCompute
