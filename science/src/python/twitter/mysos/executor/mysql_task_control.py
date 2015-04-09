import json
import os
import subprocess
import threading

from twitter.common import log
from twitter.common_internal.keybird.keybird import KeyBird
from twitter.mysos.common.decorators import synchronized

from .sandbox import Sandbox
from .task_control import TaskControl, TaskControlProvider


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
  def __init__(
      self,
      sandbox,
      framework_user,
      host,
      port,
      cluster_name,
      cluster_user,
      password,
      server_id,
      admin_keypath):
    """
      :param sandbox: The sandbox where all files of this Mysos executor instance reside.
      :param framework_user: The Unix user this framework runs as.
      :param host: The hostname of the host that runs the MySQL instance.
      :param port: The port of the MySQL instance.
      :param cluster_name: The name of the cluster.
      :param cluster_user: The Unix account that mysqld will run as and also the MySQL username.
      :param password: The MySQL password associated with 'cluster_user' in MySQL.
      :param server_id: The ID that identifies the MySQL instance.
    """
    if not isinstance(sandbox, Sandbox):
      raise TypeError("'sandbox' should be an instance of Sandbox")
    self._sandbox = sandbox

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

    self._scripts_dir = os.path.join(self._sandbox.bin, "mysql", "scripts")
    if not os.path.isdir(self._scripts_dir):
      raise TaskControl.Error("Scripts directory %s does not exist" % self._scripts_dir)

    self._conf_file = os.path.join(self._sandbox.bin, "mysql", "conf", "my.cnf")
    if not os.path.isfile(self._conf_file):
      raise TaskControl.Error("Option file %s does not exist" % self._scripts_dir)

  @synchronized
  def initialize(self, env):
    command = "%(cmd)s %(framework_user)s %(data_dir)s %(conf_file)s" % dict(
        cmd=os.path.join(self._scripts_dir, "mysos_install_db.sh"),
        framework_user=self._framework_user,
        data_dir=self._sandbox.mysql_data_dir,
        conf_file=self._conf_file)
    log.info("Executing command: %s" % command)
    subprocess.check_call(command, shell=True, env=env)

  @synchronized
  def start(self, env=None):
    if self._process:
      log.warn("start() called when a running task subprocess already exists")
      return

    command = (
        "%(cmd)s %(framework_user)s %(host)s %(port)s %(server_id)s %(data_dir)s %(log_dir)s "
        "%(tmp_dir)s %(conf_file)s %(buffer_pool_size)s" % dict(
            cmd=os.path.join(self._scripts_dir, "mysos_launch_mysqld.sh"),
            framework_user=self._framework_user,
            host=self._host,
            port=self._port,
            server_id=self._server_id,
            data_dir=self._sandbox.mysql_data_dir,
            log_dir=self._sandbox.mysql_log_dir,
            tmp_dir=self._sandbox.mysql_tmp_dir,
            conf_file=self._conf_file,
            buffer_pool_size=512 * 1024 * 1024))  # TODO(jyx): Get it from task resources.
    log.info("Executing command: %s" % command)
    self._process = subprocess.Popen(command, shell=True, env=env)

    # There is a delay before mysqld becomes available to accept requests. Wait for it.
    command = "%(cmd)s %(pid_file)s %(port)s %(timeout)s" % dict(
        cmd=os.path.join(self._scripts_dir, "mysos_wait_for_mysqld.sh"),
        pid_file=os.path.join(self._sandbox.mysql_log_dir, "mysqld.pid"),
        port=self._port,
        timeout=60)
    log.info("Executing command: %s" % command)
    subprocess.check_call(command, shell=True, env=env)

    return self._process

  @synchronized
  def reparent(self, master_host, master_port, env=None):
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
    subprocess.check_call(command, shell=True, env=env)

  @synchronized
  def promote(self, env=None):
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
    subprocess.check_call(command, shell=True, env=env)

  @synchronized
  def get_log_position(self, env=None):
    command = '%(cmd)s %(host)s %(port)s' % dict(
        cmd=os.path.join(self._scripts_dir, "mysos_log_position.sh"),
        host=self._host,
        port=self._port)

    log.info("Executing command: %s" % command)
    output = subprocess.check_output(command, shell=True, env=env).strip()

    if len(output.split(',')) == 2:
      log_file, log_position = output.split(',')  # log_file may be empty.
      log.info('Obtained log position: %s ' % str((log_file, log_position)))
      return log_file, log_position
    else:
      return None
