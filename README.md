Exercise3 instructions:

step1: start the python script to indexing the book, will be created a file GutenbergBook.csv
	- python create_doc_index.py book
	where book is the directory containing all the directories 'etextXY' containing the txt files.
	Note that I've processed manually these directory to delete all non-txt files. This directory is about 450M so I've not attached it to the homework.
	You can skip this step because I've generated at home the file GutenbergBook.csv and it is into this directory.
	
step2: normalize the tfidf of each document with the python script, will be created a file GutenbergBookNorm.csv
	- python euclidian_normalizer.py
	
step3: copy the normalized file GutenbergBook.csv into hdfs
	- hadoop fs -mkdir inputKmeans
	- hadoop fs -copyFromLocal GutenbergBook.csv /inputKmeans/GutenbergBook.txt
	
step4: compile and create jar files of the two jobs (this step culd be skipped, in the directory there are yet the jar compiled)
	- javac -classpath %HADOOP_HOME%\share\hadoop\common\hadoop-common-2.3.0.jar;%HADOOP_HOME%\share\hadoop\mapreduce\hadoop-mapreduce-client-core-2.3.0.jar;%HADOOP_HOME%\share\hadoop\common\lib\commons-cli-1.2.jar;%HADOOP_HOME%\share\hadoop\common\lib\jettison-1.1.jar *.java
	- jar -cvf KMeans.jar *.class
	
step5: start the hadoop jobs with K=10, 20, 50
	- hadoop jar KMeans.jar KMeans <K> <inputPath> <outputPath> (e.g. hadoop jar KMeans.jar KMeans 10 /inputKmeans /outKmeansK10)
		
Please note that every time we start a kmeans job, some temporary files are created in /kmeans. I did not implement any mechanism to delete this directory
because I prefer to handle these files manually. So if you launch a job again remember to delete/rename	or move the directory /kmeans.

The Hadoop jobs are 3 and I've built one main class (KMeans.class) that coordinates 2 Mapper and 3 Reducer. Specifically the Mapper/Reducer are:
	- MapperRandom: assign a random number to each doc and emit <randomNum, doc>
	- ReducerRandom: create the starting centroids
	- MapperClosestCenter: map each doc with its closest center
	- ReducerCentroid: update the centroids of each kmeans iteration
	- ReduceClusterVisualization: it is used at the end to define the cluster with the doc names. The related mapper is still MapperClosestCenter
	
If you try to start kmeans multiple times the clusters will probably not be the same.
The choice of the initial centroids change the evolution of the algorithm that bring to a local optimum, i.e. a solution that minimize the SSE from the
selected starting centroids, but it will be not probably the global optimum. To increase the performance of kmeans we could start multiple times (e.g. 10 times)
the algorithm with the same K and, by selecting every time different initial centroids, we will obtain different clusters with different SSE.
Than we choose the clusters that bring our minimum value of SSE.
How to choose K is not a trivial task. One policy is the Rule of Thumb where K~sqrt(n/2), another method is the Elbow Method that looks at the percentage
of variance explained as a function of the number of clusters.