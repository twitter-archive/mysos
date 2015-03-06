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
  """

  def __init__(self, root):
    """
      :param root: Root path of the sandbox.
    """
    self._root = root

    safe_mkdir(self.bin)
    safe_mkdir(self.lib)
    safe_mkdir(self.var)

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

  def __str__(self):
    return self._root
