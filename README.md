# Mysos
Mysos is an Apache Mesos framework for running MySQL instances. It dramatically simplifies the management of a MySQL cluster and is designed to offer:

 * Efficient hardware utilization through multi-tenancy (in performance-isolated containers)
 * High reliability through preserving the MySQL state during failure and automatic backing up to/restoring from HDFS
 * An automated self-service option for bringing up new MySQL clusters
 * High availability through automatic MySQL master failover
 * An elastic solution that allows users to easily scale up and down a MySQL cluster by changing the number of slave instances
 
Mysos is also being [proposed as a project in the Apache Incubator](https://wiki.apache.org/incubator/MysosProposal).

## Documentation
A [user guide](docs/user-guide.md) is available. Documentation improvements are always welcome, so please send patches our way.

## Getting Involved
The project maintains an IRC channel, `#mysos` on irc.freenode.net

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Requirements
 * Python 2.7
 * Mesos Python bindings

## Building and Testing
### Build/Download Mesos Python Bindings
Mysos uses Mesos Python bindings which consist of two Python packages. `mesos.interface` is on PyPI
and gets automatically installed but `mesos.native` is platform dependent. You need to either build
the package on your machine ([instructions](http://mesos.apache.org/gettingstarted/)) or download a
compiled one for your platform (e.g. Mesosphere hosts
[the eggs for some Linux platforms](https://mesosphere.com/downloads/)).

Since `pip` doesn't support eggs, you need to convert eggs into wheels using `wheel convert`, then
drop them into the `3rdparty` folder. See the [README file](3rdparty/README.md) for more
information.

### Unit Tests
Make sure [tox](https://tox.readthedocs.org/en/latest/) is installed and just run:

    tox

Tox also builds the Mysos source package and drops it in `.tox/dist`.

### End-to-end test in the Vagrant VM
The Vagrant test uses the `sdist` Mysos package in `.tox/dist` so be sure to run `tox` first. Then:

    vagrant up
    
    # Wait for the VM and Mysos API endpoint to come up (http://192.168.33.17:55001 becomes available).
    
    ./vagrant/test.sh

`test.sh` verifies that Mysos successfully creates a MySQL cluster and then deletes it.
