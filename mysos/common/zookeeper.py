import string


def parse(url):
  """
    Parse ZooKeeper URL.
    :param url: The URL in the form of "zk://username:password@servers/path".
    :return: Tuple (credential, servers, path).
             credential: Credential for authentication with "digest" scheme. Optional and default to
                         None.
             servers: Compatible with Kazoo's 'hosts' argument.
             path: Optional and default to '/'.
    NOTE: This method doesn't validate the values in the returned tuple.
  """
  index = string.find(url, "zk://")
  if index != 0:
    raise ValueError("Expecting 'zk://' at the beginning of the URL")

  url = string.lstrip(url, "zk://")

  try:
    servers, path = string.split(url, '/', 1)
  except ValueError:
    servers = url
    path = ''

  path = '/' + path

  try:
    credential, servers = string.split(servers, '@', 1)
  except ValueError:
    credential = None

  return credential, servers, path
