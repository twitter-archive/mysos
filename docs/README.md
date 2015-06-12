# Mysos
Mysos is an Apache Mesos framework for running MySQL instances. It dramatically simplifies the management of a MySQL cluster and is designed to offer:

 * Efficient hardware utilization through multi-tenancy (in performance-isolated containers)
 * High reliability through preserving the MySQL state during failure and automatic backing up to/restoring from HDFS
 * An automated self-service option for bringing up new MySQL clusters
 * High availability through automatic MySQL master failover
 * An elastic solution that allows users to easily scale up and down a MySQL cluster by changing the number of slave instances

Mysos has been [accepted into the Apache Incubator](http://incubator.apache.org/projects/mysos.html).
