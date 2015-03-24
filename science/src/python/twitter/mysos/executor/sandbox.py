from os.path import join

from twitter.common.dirutil import safe_mkdir


class Sandbox(object):
  """
    Represents the structure of the Mysos executor sandbox:

    sandbox_root/              # Sandbox for this Mysos instance.
      bin/                     # Binaries, executables.
        mysql/scripts/         # Mysos scripts (come with the executor).
          mysos_install_db.sh
          ...
      lib/                     # Libraries installed by the installer.
      var/                     # For Mysos (and MySQL) to save its state ('datadir', 'tmpdir', etc).
        mysql/                 # The sandbox maintains and exposes some standard mysql directories.
          data/
          tmp/
          logs/
  """

  def __init__(self, root):
    """
      Initializes the sandbox.

      :param root: Root path of the sandbox.

      The sandbox makes sure that the folder paths it exposes as properties are created.
    """
    self._root = root

    safe_mkdir(self.bin)
    safe_mkdir(self.lib)
    safe_mkdir(self.var)
    safe_mkdir(self.mysql_var)
    safe_mkdir(self.mysql_data_dir)
    safe_mkdir(self.mysql_tmp_dir)
    safe_mkdir(self.mysql_log_dir)

  @property
  def root(self):
    """Root path of the sandbox."""
    return self._root

  @property
  def bin(self):
    """For Mysos binaries."""
    return join(self._root, "bin")

  @property
  def lib(self):
    """For libraries that the Mysos executor replies on."""
    return join(self._root, "lib")

  @property
  def var(self):
    """For the Mysos executor's application data."""
    return join(self._root, "var")

  @property
  def mysql_var(self):
    """For mysql related data files."""
    return join(self.var, 'mysql')

  @property
  def mysql_data_dir(self):
    return join(self.mysql_var, 'data')

  @property
  def mysql_tmp_dir(self):
    return join(self.mysql_var, 'tmp')

  @property
  def mysql_log_dir(self):
    return join(self.mysql_var, 'logs')

  def __str__(self):
    return self._root
