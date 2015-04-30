"""Set of utility functions for working with OS commands.

Functions in this module return the command string. These commands are composed but not executed.
"""

import os
from subprocess import call


HADOOP_CONF_DIR = '/etc/hadoop/conf'


def encrypt(key_file):
  """
    Encrypt the data from stdin and write output to stdout.

    :param key_file: The key file used to encrypt the stream.
  """
  if not os.path.isfile(key_file):
    raise ValueError("Cannot find key_file: %" % key_file)

  return "openssl aes-256-cbc -salt -pass file:%s" % key_file


def decrypt(key_file):
  """
    Decrypt the data from stdin and write output to stdout.

    :param key_file: The key file used to decrypt the stream.
  """
  if not os.path.isfile(key_file):
    raise ValueError("Cannot find key_file: %" % key_file)

  return "openssl aes-256-cbc -d -pass file:%s" % key_file


def compress(extension):
  """
    Compress the data from stdin and write output to stdout.
    :param extension: The compression format identified by the file extension. Allowed values are:
                      'gz' for gzip, 'bz' or 'bz2' for bzip.
  """
  if extension == "gz":
    cmd = "pigz" if exists("pigz") else "gzip"
  elif extension == "bz" or extension == "bz2":
    cmd = "bzip2"
  elif extension == 'lzo':
    cmd = "lzop"
  else:
    raise ValueError("Unknown compression format/file extension")

  return cmd


def decompress(extension):
  """
    Decompress the data from stdin and write output to stdout.

    :param extension: The compression format identified by the file extension. Allowed values are:
                      'gz' for gzip, 'bz' or 'bz2' for bzip.
  """
  if extension == "gz":
    cmd = "pigz -d" if exists("pigz") else "gzip -d"
  elif extension == "bz" or extension == "bz2":
    cmd = "bzip2 -d"
  elif extension == 'lzo':
    cmd = "lzop -d"
  else:
    raise ValueError("Unknown compression format/file extension")

  return cmd


def hdfs_cat(uri, conf=HADOOP_CONF_DIR):
  """
    Fetch the data from the specified uri and write output to stdout.

    :param uri: The HDFS URI.
    :param conf: The hadoop config directory.
  """
  return "hadoop --config %s dfs -cat %s" % (conf, uri)


def pv(size):
  """
    Monitor the progress of data through a pipe. If 'pv' is not available, simply 'cat' it.

    :param size: The size of the data, to calculate percentage.
  """
  if exists('pv'):
    return "pv --wait --size %s" % size
  else:
    return "cat"


def untar(directory):
  """
    Untar the data from stdin into the specified directory.

    :param directory: The directory to write files to.
  """
  return "tar -C %s -x" % directory


def tar(path):
  """
    Tar the path and write output to stdout.

    :param path: All contents under path are 'tar'ed.
  """
  if not os.path.exists(path):
    raise ValueError("Invalid argument: 'path' doesn't exist")

  path = path.rstrip(os.sep)
  parent, base = os.path.split(path)
  return "tar -C %s %s" % (parent, base)


def exists(cmd):
  """Return true if 'cmd' exists in $PATH."""
  with open(os.devnull, "w") as f:
    return call(['which', cmd], stdout=f) == 0  # No stdout.
