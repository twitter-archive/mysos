from twitter.mysos.common.zookeeper import parse

import pytest


def test_parse():
  assert parse("zk://host1:port1") == (None, "host1:port1", "/")
  assert parse("zk://jake:1@host1:port1") == ("jake:1", "host1:port1", "/")
  assert parse("zk://jake:1@host1:port1/") == ("jake:1", "host1:port1", "/")
  assert (parse("zk://jake:1@host1:port1,host2:port2") ==
          ("jake:1", "host1:port1,host2:port2", "/"))
  assert (parse("zk://jake:1@host1:port1,host2:port2/") ==
          ("jake:1", "host1:port1,host2:port2", "/"))
  assert (parse("zk://jake:1@host1:port1,host2:port2/path/to/znode") ==
          ("jake:1", "host1:port1,host2:port2", "/path/to/znode"))


def test_parse_errors():
  with pytest.raises(ValueError) as e:
    parse("host1:port1")
  assert e.value.message == "Expecting 'zk://' at the beginning of the URL"

  # This method doesn't validate the values in the tuple.
  assert parse("zk://") == (None, "", "/")
  assert parse("zk://host_no_port") == (None, "host_no_port", "/")
  assert parse("zk://jake@host") == ("jake", "host", "/")
