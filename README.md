# Mysos
Mysos is an Apache Mesos framework for running MySQL instances. It dramatically simplifies the management of a MySQL cluster and is designed to offer:

 * Efficient hardware utilization through multi-tenancy (in performance-isolated containers)
 * High reliability through preserving the MySQL state during failure and automatic backing up to/restoring from HDFS
 * An automated self-service option for bringing up new MySQL clusters
 * High availability through automatic MySQL master failover
 * An elastic solution that allows users to easily scale up and down a MySQL cluster by changing the number of slave instances
 
Mysos is also being [proposed as a project in the Apache Incubator](https://wiki.apache.org/incubator/MysosProposal).

## Documentation
For convenience of getting started locally, there is a vagrant file which can be executed by running `vagrant up` on your local machine. A [user guide](docs/user-guide.md) is also available. Documentation improvements are always welcome, so please send patches our way.

## Getting Involved
The project maintains an IRC channel, `#mysos` on irc.freenode.net

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
