"""This module is specific to Twitter's practice of distributing MySQL package.

TODO(jyx): Twitter specific files should be organized together in a 'twitter/internal' folder.
"""
import json
import os
import tarfile

from twitter.common import log
from twitter.mysos.common.fetcher import Fetcher, FetcherFactory
from twitter.mysos.common.hdfs import HDFSFetcher

from .installer import PackageInstaller, PackageInstallerProvider
from .sandbox import Sandbox


MYSQL_PKG_FILE = 'mysos_mysql.tar.gz'  # The tarball is custom packaged for Mysos.


FetcherFactory.register_fetcher('hdfs', HDFSFetcher())
FetcherFactory.register_fetcher('hftp', HDFSFetcher())


class TwitterPackageInstallerProvider(PackageInstallerProvider):
  """
    At Twitter, a custom portable MySQL package is used and it is hosted on HDFS.
  """

  def from_task(self, task, sandbox):
    data = json.loads(task.data)

    if 'installer_args' not in data or not data['installer_args']:
      raise ValueError(
        "Cannot create TwitterPackageInstaller because 'installer_args' is not provided in task "
        "data")

    installer_args = json.loads(data['installer_args'])

    if not installer_args or 'mysql_pkg_uri' not in installer_args:
      raise PackageInstaller.Error(
          "Cannot create TwitterPackageInstaller because 'mysql_pkg_uri' is not provided in "
          "'installer_args'")

    return TwitterPackageInstaller(installer_args['mysql_pkg_uri'], sandbox)


class TwitterPackageInstaller(PackageInstaller):
  """
    This installer installs the following artifacts into the sandbox:

    sandbox_root/
      mysos_mysql.tar.gz       # MySQL package file dropped here by the fetcher.
      bin/
        mysql/scripts/
          mysos_install_db.sh
          ...
      lib/
        mysql/                 # The MySQL package extracted from 'mysos_mysql.tar.gz'.
          bin/
          scripts/
          ...
        auxiliary libraries    # Other libraries from 'mysos_mysql.tar.gz'.
      var/

    See comments on Sandbox for more info.
  """

  def __init__(self, mysql_pkg_uri, sandbox):
    self._mysql_pkg_uri = mysql_pkg_uri

    if not isinstance(sandbox, Sandbox):
      raise TypeError("'sandbox' should be an instance of Sandbox")
    self._sandbox = sandbox

    # The package should be fetched into the sandbox root.
    self._pkg_path = os.path.join(self._sandbox.root, MYSQL_PKG_FILE)

    # As in mysqld --basedir.
    self._mysql_basedir = os.path.join(self._sandbox.lib, "mysql")

  def install(self):
    try:
      fetcher = FetcherFactory.get_fetcher(self._mysql_pkg_uri)
      fetcher.fetch(self._mysql_pkg_uri, self._sandbox.root)
      log.info("Successfully fetched task package into %s" % self._sandbox)
    except Fetcher.Error as e:
      raise self.Error("Failed to install package due to: %s" % e)

    log.info("Extracting %s" % self._pkg_path)
    # TODO(jyx): Parallelize it?
    with tarfile.open(self._pkg_path, 'r') as tf:
      tf.extractall(path=self._sandbox.lib)

    return dict(
        PATH=os.pathsep.join([
            os.path.join(self._mysql_basedir, 'bin'),  # mysqld, etc.
            os.path.join(self._mysql_basedir, 'scripts'),  # mysql_install_db.
            os.environ.get('PATH', '')]),
        LD_LIBRARY_PATH=os.pathsep.join([
            self._sandbox.lib,  # For auxiliary libs.
            os.environ.get('LD_LIBRARY_PATH', '')]))
