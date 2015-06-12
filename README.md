# Mysos
Mysos is an Apache Mesos framework for running MySQL instances. It dramatically simplifies the management of a MySQL cluster and is designed to offer:

 * Efficient hardware utilization through multi-tenancy (in performance-isolated containers)
 * High reliability through preserving the MySQL state during failure and automatic backing up to/restoring from HDFS
 * An automated self-service option for bringing up new MySQL clusters
 * High availability through automatic MySQL master failover
 * An elastic solution that allows users to easily scale up and down a MySQL cluster by changing the number of slave instances
 
Mysos has been [accepted into the Apache Incubator](http://incubator.apache.org/projects/mysos.html).

## Documentation
A [user guide](docs/user-guide.md) is available. Documentation improvements are always welcome, so please send patches our way.

## Getting Involved
The project maintains an IRC channel, `#mysos` on irc.freenode.net

### Mailing Lists
- [Development Mailing List](mailto:dev-subscribe@mysos.incubator.apache.org)
([Archives](http://www.mail-archive.com/dev@mysos.incubator.apache.org/))

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Requirements
 * Python 2.7
 * Mesos Python bindings

## Building
[![Build status on Travis CI](https://api.travis-ci.org/twitter/mysos.svg)](https://travis-ci.org/twitter/mysos)

### Building/Downloading Mesos Python Bindings
Mysos uses Mesos Python bindings which consist of two Python packages. `mesos.interface` is on PyPI
and gets automatically installed but `mesos.native` is platform dependent. You need to either build
the package on your machine ([instructions](http://mesos.apache.org/gettingstarted/)) or download a
compiled one for your platform (e.g. Mesosphere hosts
[the eggs for some Linux platforms](https://mesosphere.com/downloads/)).

Since `pip` doesn't support eggs, you need to convert eggs into wheels using `wheel convert`, then
drop them into the `3rdparty` folder. See the [README file](3rdparty/README.md) for more
information.

### Building Mysos
Mysos mainly consists of two components that are built and deployed separately.

- `mysos_scheduler`: The scheduler that connects to Mesos master and manages the MySQL clusters.
- `mysos_executor`: The executor that is launched by Mesos slave (upon `mysos_scheduler`'s request)
to carry out MySQL tasks.

One way to package these components and their dependencies into a self-contained executable is to
use [PEX](https://pex.readthedocs.org/en/latest/). This allow Mysos components to be launched
quickly and reliably. See
[End-to-end test using PEX](#end-to-end-test-on-a-local-mesos-cluster-and-pex) for an example of
packaging and deploying the executor using PEX.

## Testing
### Unit Tests
Make sure [tox](https://tox.readthedocs.org/en/latest/) is installed and just run:

    tox

The unit tests don't require the `mesos.native` package to be available in `3rdparty`. Tox also
builds the Mysos source package and drops it in `.tox/dist`.

### End-to-end Test on a Local Mesos Cluster and PEX
Build/download the `mesos.native` package and put it in `3rdparty` and then run:

    tox -e pex

This test demonstrates how to package a PEX executor and use it to launch a *fake* MySQL cluster on
a *local* Mesos cluster.

### End-to-end Test on a Real Mesos Cluster in a Vagrant VM
The Vagrant test uses the `sdist` Mysos package in `.tox/dist` so be sure to run `tox` first. Then:

    vagrant up
    
    # Wait for the VM and Mysos API endpoint to come up (http://192.168.33.17:55001 becomes available).
    
    tox -e vagrant

`test.sh` verifies that Mysos successfully creates a MySQL cluster and then deletes it.
