# Spark Join Partitioning Performance
This is the code used in my bachelor's thesis to benchmark different Spark join methods (RDD, DataFrame, Dataset) and compare the influence of partitioning on each of the methods.
## Background
Due to the ever rising amount of data specialized systems began to surface that can do a specific operation really well. Eventually there arose such an overhead through the communication between different systems that a unified solution was required. As a result of this Apache Spark was started, which enables users to perform operations that previously required multiple systems.
## State of the Art

Apache Spark provides a Scala  API that allows a user to communicate with a Spark cluster. The framework takes care of error handling and the distribution of tasks to worker nodes. It also partitions the data across the network but does let the user take influence over how this is done. In a distributed system such as Apache Spark data placement has a huge impact on performance. The way data is distributed or partitioned across the network is one of the biggest reasons for slow execution times.
## Goal
In this thesis we want to analyze the performance of different join methods under Apache Spark and compare their results to each other. For this we will take a special look at partitioning and how data partitioned by a specific column behaves from a performance point of view.
## Method
We do this through empirical experiments and analyze a join between two big tables and also a join between a big and a small table. This repository features the code used in the experiments.
## Result
We show that the data type used makes a difference and there is a lot of performance to be gained from properly partitioning data. It is also very important to know how the data to be processed is structured, as different methods perform better depending on the actual data given.
