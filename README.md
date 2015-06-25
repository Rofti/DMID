# DMID
Implementation of the overlapping community detection algorithm DMID for giraph as part of the bachelor thesis "Pregel: Parallel Implementation of Overlapping Community Detection Algorithms" at the chair of Information Systems RWTH Aachen University.


Command:
$HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples/target/giraph-examples-1.1.0-for-hadoop-1.2.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.DMIDComputation -vif org.apache.giraph.examples.io.formats.DMIDVertexInputFormat -vip /input/graphOutput.txt -vof org.apache.giraph.examples.io.formats.DMIDVertexOutputFormat -op /output -w 1 -mc org.apache.giraph.examples.utils.DMIDMasterCompute

##SETUP
In the following, we will describe how to set up and configure Hadoop 1.2.1 and Giraph 1.1.0 on Ubuntu 64-bit.

- Prerequisites:

    **Java 1.6 or later:**
    Check installed version:
    ```
    $ java -version
    ```
    Install Java 7:
    ```
    $ sudo apt-get install openjdk-7-jdk
    ```
    **SSH:**
    If not installed, use:
    ```
    $ sudo apt-get install ssh
    ```
    **Hadoop 1.2.1:**
    Download hadoop (Ex. hadoop-1.2.1.tar.gz). You can find the version here:     
    https://www.apache.org/dyn/closer.cgi/hadoop/common/
    
    **Maven 3 or later**
    
    **Giraph**
    Download latest stable giraph from https:
    http://giraph.apache.org/releases.html 
  
###Install hadoop in a single-node, pseudo distribution on Ubuntu


1. Create a system user and password for hadoop:

    ```
    $ sudo addgroup hadoop
    $ sudo adduser –ingroup hadoop hduser
    ```
2. SSH configuration

    Change user to hduser:
    ```
    $ su – hduser
    ```
    Enable access to local machine with newly created ssh key:
    ```
    $ ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    $ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    $ ssh localhost
    ```
3. Extract hadoop sources: 

    Unpack the .tar.gz: 
    ```
    $ sudo tar xzf hadoop-1.2.1.tar.gz
    ```
    Move the unpacked file to */usr/local/hadoop*:
    ```
    $ sudo mv /hadoop-1.2.1 /usr/local/hadoop
    $ sudo cd /usr/local
    ```
    Make hduser:hadoop its owner using:
    ```
    $ sudo chown -R hduser:hadoop hadoop
    ```
4. Configure hadoop for single node setup using:

    + Edit *conf/core-site.xml* and insert following within configuration tag:
    ```xml
    <property>
	      <name>fs.default.name</name>
	      <value>hdfs://localhost:9000/</value>
    </property>
    	
    <property>
        <name>dfs.permissions</name>
	      <value>false</value>
    </property>
    	
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/app/hadoop/tmp</value>
        <description>A base for other temporary directories.</description>
    </property>
    ```
    + The third property of the core-site.xml file describes the default base temporary directory for
      the local file system and HDFS which is /app/hadoop/tmp. So ownership and permission are required:
    ```
    $ sudo mkdir -p /app/hadoop/tmp
    $ sudo chown -R hduser:hadoop /app/hadoop/tmp
    ```
    + Edit *conf/hdfs-site.xml* and insert following within configuration tag:
    ```xml
    <property>
       <name>dfs.data.dir</name>
       <value>/home/hduser/mydata/hdfs/datanode</value>
       <final>true</final>
    </property>
    	
    <property>
    	  <name>dfs.name.dir</name>
    	  <value>/home/hduser/mydata/hdfs/namenode</value>
    	  <final>true</final>
    </property>
    	
    <property>
    	  <name>dfs.replication</name>
    	  <value>1</value>
    </property>
    ```
    + Edit *conf/mapred-site.xml* and following within configuration tag:
    ```xml  
    <property>
  	    <name>mapred.job.tracker</name>
  	    <value>localhost:9001</value>
    </property>
  	
    <property>
       <name>mapred.tasktracker.map.tasks.maximum</name>
       <value>4</value>
    </property>
  
    <property>
       <name>mapred.map.tasks</name>
       <value>4</value>
    </property>
	  ```
    + Edit *conf/hadoop-env.sh* and do following changes:
        - Uncomment JAVA_HOME and set it to an appropriate value based on installed java.
        ```  
        export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
        ```  
        - Uncomment HADOOP_OPTS and set it to:
        ```  
        export HADOOP_OPTS=-Djava.net.preferIPv4Stack=true
        ```
5. Format namenode:

    ```
    $ ./bin/hadoop namenode -format
    ```
6. Start the services:

    ```
    $ ./bin/start-all.sh
    ```
7. Check that all services got started using ‘jps’ command. Output should be similar with different process-ids:

    ```
    $ jps
    
    26049 SecondaryNameNode
  	25929 DataNode
  	26399 Jps
  	26129 JobTracker
  	26249 TaskTracker
  	25807 NameNode
    ```
8. Try to access different services at:

    - For the Jobtracker:
        http://localhost:50030/
    - For the Namenode:
        http://localhost:50070/
    - For the Tasktracker:
        http://localhost:50060/
9. Run an example

    + Download large text file for example :

    ```
    wget http://www.gutenberg.org/cache/epub/132/pg132.txt
    ```
    
    + Check that all services are running. 
    + Create directory /input in hdfs and copy the textfile from the local filesystem to hdfs:
    
    ```
    $ bin/hadoop dfs -mkdir /input
    $ bin/hadoop dfs -copyFromLocal pg132.txt /input/pg132.txt
    ```
    - Verify that the file got copied:
    
    ```
    $ bin/hadoop dfs -ls /input
    ```
    - Find more commands with: 
    
    ```
    $ bin/hadoop dfs help
    ```
    + Start hadoop wordcount example:
    
    ```
    $ bin/hadoop jar ../hadoop/hadoop-examples-1.2.1.jar wordcount /input/pg132.txt /output/wordcount
    ```
    + Check the output with:
    
    ```
    $ bin/hadoop dfs -cat /output/wordcount/p* | less
    ```
10. To stop all services use: 

    ```
    $ ./bin/stop-all.sh
    ```
    
    
### Install giraph for hadoop
1. If not formatted already, format the hadoop namenode:

    ```
    $ ./bin/hadoop namenode -format
    ```
2. Start all hadoop services:

    ```
    $ ./bin/start-all.sh
    ```
3. Extract giraph source in */usr/local/giraph* folder.
4. Make sure giraph files are owned by *hduser:hadoop*:
    
    ```
    $ sudo chown -R hduser:hadoop giraph
    ```
5. Edit *~/.bashrc* for the hduser user account and add:

    ```
    export GIRAPH_HOME=/usr/local/giraph
    ```
6. Save and close the file. After that do:

    ```
    $ source $HOME/.bashrc
    $ cd $GIRAPH_HOME
    ```
    
7. Build giraph using maven (This will take quite a long time):
    ```
    $ mvn package -DskipTests
    ```
8. The folder *'giraph-core/target'* should afterwards have a file named *'giraph-<ver>-for-hadoop-<ver>-jar-with-dependencies.jar'*. Also folder *'giraph-examples/target/'* would have a jar file for examples with similar naming. In our case this would be *'giraph-examples-1.1.0-for-hadoop-1.2.1-jar-with-dependencies.jar'*

9. Testing giraph by running a simple giraph job
    + create an input file named *'graph.txt'* with the following data:
    
        ```
        [0,0,[[1,1],[3,3]]]
        [1,0,[[0,1],[2,2],[3,1]]]
        [2,0,[[1,2],[4,4]]]
        [3,0,[[0,3],[1,1],[4,4]]]
        [4,0,[[3,4],[2,4]]]
        ```
        Each line has the format *'[source_id,source_value,[[dest_id, edge_value],...]]'*. 
    + Copy the input file to HDFS:
        ```
        $ bin/hadoop dfs -copyFromLocal graph.txt /input/graph.txt
        ```
    + Run the simple shortest path giraph example:
        
        ```
        usr/local/hadoop/bin/hadoop jar
        /usr/local/giraph/giraph-examples/target/giraph-examples-1.1.0-for-hadoop-1.2.1-jar-with-dependencies.jar 
        org.apache.giraph.GiraphRunner
        org.apache.giraph.examples.SimpleShortestPathsComputation
        -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat
        -vip /input/graph.txt
        -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat
        -op /output/shortestpaths
        -w 1
        ```
     + To check the output, use: 
        ```
        $ bin/hadoop dfs -cat /output/shortestpaths/p* 
        ```
        
        You should have this output: 
        ```
        0  1.0
	2  2.0
        1  0.0
        3  1.0
        4  5.0
        ```
9. To stop all services use: 

    ```
    $ ./bin/stop-all.sh
    ```
