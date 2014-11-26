import json
import os
import subprocess
import tarfile
import threading

from twitter.common import log
from twitter.common.dirutil import safe_mkdir
from twitter.common_internal.keybird.keybird import KeyBird
from twitter.mysos.common.decorators import synchronized

from .task_control import TaskControl, TaskControlProvider


MYSQL_PKG_FILE = 'mysos_mysql.tar.gz'


class MySQLTaskControlProvider(TaskControlProvider):
  """
    The default implementation of MySQLTaskControlProvider.
    There exist other implementations for testing purposes.
  """

  def from_task(self, task, sandbox):
    data = json.loads(task.data)

    # TODO(jyx): Use an ephemeral sandbox for now. Will change when Mesos adds persistent resources
    # support: MESOS-1554.
    return MySQLTaskControl(
        sandbox,
        data['framework_user'],
        data['host'],
        data['port'],
        data['cluster'],
        data['cluster_user'],
        data['cluster_password'],
        data['server_id'],
        data['admin_keypath'])


class MySQLTaskControl(TaskControl):
  """
    MySQL task control expects the following directory hierarchy:

    mysos_home/                # Home to this Mysos instance (i.e. executor sandbox).
      mysos_mysql.tar.gz       # MySQL package file dropped here by the executor.
      bin/                     # Binaries, executables.
        mysql/scripts/         # Mysos scripts.
          mysos_install_db.sh
          ...
      lib/
        mysql/                 # The MySQL package extracted from 'mysos_mysql.tar.gz'.
          bin/
          scripts/
          ...
        auxiliary libraries    # Other libraries.
      var/                     # For MySQL data dir, tmp dir, etc.
  """

  def __init__(
      self,
      mysos_home,
      framework_user,
      host,
      port,
      cluster_name,
      cluster_user,
      password,
      server_id,
      admin_keypath):
    """
      :param mysos_home: The home directory where the mysos instance directories reside.
      :param framework_user: The Unix user this framework runs as.
      :param host: The hostname of the host that runs the MySQL instance.
      :param port: The port of the MySQL instance.
      :param cluster_name: The name of the cluster.
      :param cluster_user: The Unix account that mysqld will run as and also the MySQL username.
      :param password: The MySQL password associated with 'cluster_user' in MySQL.
      :param server_id: The ID that identifies the MySQL instance.
    """
    self._mysos_home = mysos_home
    self._framework_user = framework_user
    self._host = host
    self._port = port
    self._cluster_name = cluster_name
    self._cluster_user = cluster_user
    self._password = password
    self._server_id = server_id

    try:
      keybird = KeyBird(admin_keypath)
    except KeyBird.KeyBirdException as e:
      raise TaskControl.Error("Unable to obtain admin credentials: %s" % e)
    self._admin_username = keybird.get_creds("username")
    self._admin_password = keybird.get_creds("password")
    log.info("Loaded credentials for admin account %s" % self._admin_username)

    self._lock = threading.Lock()
    self._process = None  # The singleton task process that launches mysqld.

    self._scripts_dir = os.path.join(mysos_home, "bin", "mysql", "scripts")
    if not os.path.isdir(self._scripts_dir):
      raise TaskControl.Error("Scripts directory %s does not exist" % self._scripts_dir)

    self._pkg_path = os.path.join(self._mysos_home, MYSQL_PKG_FILE)
    self._lib_dir = os.path.join(self._mysos_home, 'lib')
    safe_mkdir(self._lib_dir)
    self._mysql_basedir = os.path.join(self._lib_dir, "mysql")
    self._var_dir = os.path.join(self._mysos_home, "var")
    safe_mkdir(self._var_dir)

    self._mysql_env = dict(
        PATH=os.pathsep.join([
            os.path.join(self._mysql_basedir, 'bin'),  # mysqld, etc.
            os.path.join(self._mysql_basedir, 'scripts'),  # mysql_install_db.
            os.environ.get('PATH', '')]),
        LD_LIBRARY_PATH=os.pathsep.join([
            self._lib_dir,  # For auxiliary libs.
            os.environ.get('LD_LIBRARY_PATH', '')]))

  @synchronized
  def start(self):
    if self._process:
      return

    log.info("Extracting %s" % self._pkg_path)
    with tarfile.open(self._pkg_path, 'r') as tf:
      tf.extractall(path=self._lib_dir)

    command = "%(cmd)s %(cluster_name)s %(port)s %(framework_user)s %(var_dir)s" % dict(
        cmd=os.path.join(self._scripts_dir, "mysos_install_db.sh"),
        cluster_name=self._cluster_name,
        port=self._port,
        framework_user=self._framework_user,
        var_dir=self._var_dir)
    log.info("Executing command: %s" % command)
    subprocess.check_call(command, shell=True, env=self._mysql_env)

    command = ('%(cmd)s %(cluster_name)s %(host)s %(port)s %(framework_user)s %(server_id)s '
        '%(var_dir)s' % dict(
            cmd=os.path.join(self._scripts_dir, "mysos_launch_mysqld.sh"),
            cluster_name=self._cluster_name,
            host=self._host,
            port=self._port,
            framework_user=self._framework_user,
            server_id=self._server_id,
            var_dir=self._var_dir))
    log.info("Executing command: %s" % command)
    self._process = subprocess.Popen(command, shell=True, env=self._mysql_env)

    # There is a delay before mysqld becomes available to accept requests. Wait for it.
    command = "%(cmd)s %(pid_file)s %(port)s %(timeout)s" % dict(
      cmd=os.path.join(self._scripts_dir, "mysos_wait_for_mysqld.sh"),
      pid_file=os.path.join(self._var_dir, self._cluster_name, str(self._port), "mysqld.pid"),
      port=self._port,
      timeout=10)
    log.info("Executing command: %s" % command)
    subprocess.check_call(command, shell=True, env=self._mysql_env)

    return self._process

  @synchronized
  def reparent(self, master_host, master_port):
    command = ("%(cmd)s %(master_host)s %(master_port)s %(slave_host)s %(slave_port)s "
        "%(admin_user)s %(admin_password)s" % dict(
            cmd=os.path.join(self._scripts_dir, "mysos_reparent.sh"),
            master_host=master_host,
            master_port=master_port,
            slave_host=self._host,
            slave_port=self._port,
            admin_user=self._admin_username,
            admin_password=self._admin_password))

    log.info("Executing command: %s" % command)
    subprocess.check_call(command, shell=True, env=self._mysql_env)

  @synchronized
  def promote(self):
    command = ("%(cmd)s %(host)s %(port)s %(cluster_user)s %(password)s %(admin_user)s "
        "%(admin_password)s" % dict(
            cmd=os.path.join(self._scripts_dir, "mysos_promote_master.sh"),
            host=self._host,
            port=self._port,
            cluster_user=self._cluster_user,
            password=self._password,
            admin_user=self._admin_username,
            admin_password=self._admin_password))

    # TODO(jyx): Scrub the command log line to hide the password.
    log.info("Executing command: %s" % command)
    subprocess.check_call(command, shell=True, env=self._mysql_env)

  @synchronized
  def get_log_position(self):
    command = '%(cmd)s %(host)s %(port)s' % dict(
        cmd=os.path.join(self._scripts_dir, "mysos_log_position.sh"),
        host=self._host,
        port=self._port)

    log.info("Executing command: %s" % command)
    output = subprocess.check_output(command, shell=True, env=self._mysql_env).strip()

    if len(output.split(',')) == 2:
      log_file, log_position = output.split(',')  # log_file may be empty.
      log.info('Obtained log position: %s ' % str((log_file, log_position)))
      return log_file, log_position
    else:
      return None
