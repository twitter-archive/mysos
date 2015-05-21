# Using Mysos

Mysos provides a REST API for creating and managing MySQL clusters on Mesos.

## Dashboard
- HTTP Method: `GET`
- Path:  `/`

The root of API endpoint is a web page that lists the managed MySQL clusters.

## Creating a MySQL cluster
- HTTP Method: `POST`
- Path: `/clusters/<cluster_name>`


### Parameters
- `cluster_name`: Required. Name of the cluster.
- `cluster_user`: Required. The user account for all MySQL instances in the cluster which as full
admin privileges.
- `num_nodes`: Number of nodes in the cluster. [default: 1]
- `size`: The size of instances in the cluster as a JSON dictionary of `cpus`, `mem` and `disk`.
`mem` and `disk` are specified with standard data size units such as `mb`, `gb`, `tb`, etc. (no
spaces, see the default for an example) [default: `{"mem": "512mb", "disk": "2gb", "cpus": 1.0}`]
- `backup_id`: An ID for the MySQL backup to restore from when the MySQL instance starts. If not
specified, Mysos will start an empty MySQL instance. The format and meaning of `backup_id` is
specific to the implementation of `BackupStore` that the Mysos cluster uses.
- `cluster_password`: The password used for accessing MySQL instances in the cluster as well as
deleting the cluster from Mysos. If unspecified then Mysos generates one for the cluster. In either
case the password is sent back as part of the response.

`cluster_name` is part of the path and the rest of the parameters are specified as form fields.


### Response
A JSON object with the following fields:

- `cluster_password`: The password for accessing the MySQL instance (associated with
`cluster_user`).
- `cluster_url`: A URL to the ZooKeeper group for discovering the MySQL instances of this cluster.
See the *Service Discovery* section below.


### Example

    # Create a cluster named 'test_cluster3' and restore from the backup 'foo/bar:201503122000'.
    curl -X POST 192.168.33.7/clusters/test_cluster3 --form "cluster_user=mysos" \
      --form "num_nodes=2" --form "backup_id=foo/bar:201503122000" \
      --form 'size={"mem": "512mb", "disk": "3gb", "cpus": 1.0}'
      
    # Response
    {"cluster_password": "w9gMCkecsMh6sWsRdxNTa", "cluster_url": "zk://192.168.33.7:2181/mysos/discover/test_cluster3"}


### Notes
- Cluster creation is asynchronous. The API call returns (with status 200) as soon as the Mysos
scheduler has accepted the request. The same goes for cluster deletion.
- ZooKeeper `<cluster_url>/master` sub-group has at most one ZNode which is the master of the MySQL
cluster.
- ZooKeeper `<cluster_url>/slaves` sub-group can have multiple ZNodes which are the slaves of the
MySQL cluster.
- A ZNode is added to the ZooKeeper group when the instance becomes available and ready to serve
traffic.

## Removing a MySQL cluster
- HTTP Method: `DELETE`
- Path: `/clusters/<cluster_name>`

### Parameters
- `cluster_name`: Name of the cluster.
- `password`: The password for the cluster returned by cluster creation call.

### Response
A JSON object with:

- `cluster_url`: A URL to the ZooKeeper group to watch for the termination of the cluster. The group
 ZNode is removed from ZooKeeper when the MySQL cluster is removed/terminated.

### Example
```
# Remove a cluster named 'test_cluster3'
curl -X DELETE 192.168.33.7/cluster/test_cluster3 --form "password=w9gMCkecsMh6sWsRdxNTa"
# Response
{"cluster_url": "zk://mysos:mysos@192.168.33.7:2181/mysos/discover/test_cluster3"}
```

## Service Discovery
Mysos' service discovery with ZooKeeper conforms to the ServerSet protocol. Each MySQL instance is
represented by a ZNode with its data being a
[ServiceInstance](https://github.com/twitter/commons/blob/master/src/thrift/com/twitter/thrift/endpoint.thrift)
serialized into JSON.

- The `Endpoint serviceEndpoint` field in ServiceInstance has the `host` and `port` that MySQL
client can connect to.
- Some utilities for watching ZooKeeper and parsing the `ServiceInstance`s:
[Java src](https://github.com/twitter/commons/tree/master/src/java/com/twitter/common/zookeeper) |
[Maven](http://maven.twttr.com/com/twitter/zookeeper-client/LATEST/),
[Python src](https://github.com/twitter/commons/tree/master/src/python/twitter/common/zookeeper) |
[PyPI](https://pypi.python.org/pypi/twitter.common.zookeeper/).
