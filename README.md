# Hazelnut


Hazelnut is a process to run on each node in cluster and to perform start-up activity once in cluster lifetime. It also keep updating the cluster status after fixed delay. 

# Requirement Specification


Imagine an environment that consists of multiple nodes. Each node is a separate JVM process and could potentially be running on a distinct physical machine. Your task is to write an application that will run on all nodes. The application should coordinate between the nodes so that as they are started System.out.println("We are started!") is only called exactly once across the whole cluster, whether 1 node or 10 are running.
<br/><br/>As we are discussing a distributed environment, your solution should take into consideration the variables that such an environment presents, such as:<br/>
- Some nodes may start at the exact same time as others.<br/>
- Some nodes may start seconds or minutes later than others.<br/>
- Some nodes may restart before others start a first time.<br/>
- Some nodes may never be started at all.<br/><br/>
There is no need to build a distributed system from scratch. You can use an already existing library for the solution. We highly value simplicity.

Please Note that it was advised in answer to questions:<br/> 
- That in case of network failures the Node should go ahead with printing message.<br/> 
- That in case of whole cluster restart the Node should go ahead with printing message. i.e. if all nodes went down again then one node getting up after, should print message again.

# Implementation

Please have a look at flow chart of implementation. 

<br/><br/>
![aa](https://raw.githubusercontent.com/m-khayyam/hazelnut/main/src/main/resources/hazelnut-startup-flow.jpeg)
<br/><br/>

- Apache ZooKeeper is used as cluster's center point.<br/>
- Curator API is used for simple communication with ZooKeeper<br/>
- DistributedLock.java and ZooKeeperSession.java are wrappers on CuratorFramework APIs for simplicity.<br/>
- NodeStarup.java and NodeLivenessReporter.java are main classes taking care of business logic.<br/><br/>
In following scenarios the starting up node prints welcome message:
- Cluster is never marked before as started and No other node is printing the message in paralal<br/>
- Cluster is marked as started before but now no other node is connected. Thus getting this node up is kind of restart<br/>
- The node is unable to connect to cluster due to network issue.
</a>
# Implementation Limitations or Pending Work
- No authentication on ZooKeeper Connectivity
- Properties are not validated
- Rest of the code is unit tested but not ZooKeeperSession(part of project not third party). Short of time.
- Caveat: Catched **Exception** multiple places because Curator Framework or ZooKeeper throw Exception instead of specific exceptions.
- Caveat: Exceptions are mostly just logged and handled within the methods. Because flow is supposed to continue pessimistically(network issue => cluster not up). And the caller don't need to decide anything on exception. 

  
# Application and ZooKeeper Server Startup Guide

- Download the latest stable copy of Apache ZooKeeper server from <a>https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz</a><br/>
- Extract the gz file<br/>
- Go to apache-zookeepr*/conf dir and rename zoo_sample.cfg file to zoo.cfg<br/>
- Add the  properties extendedTypesEnabled=true and emulate353TTLNodes=true to zoo.cfg file<br/>
- Go to apache-zookeepr*/bin and Run zkServer.com for windows or zkServer.sh for linux, to start the server<br/>

- Set the properties for Hazelnut process in application.properties as desired. Make sure zookeeper port is sat as same as in zoo.cfg above<br/>
- Run the application as the main method is in HazelnutApplication.java.
