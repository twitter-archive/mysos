from .installer import PackageInstaller, PackageInstallerProvider


class NoopPackageInstaller(PackageInstaller):
  """
    An installer that doesn't actually install the package.

    It can be used when the host has dependent packages (i.e. MySQL) pre-installed and its upgrades
    managed externally.
  """

  def install(self):
    return None  # No environment variables to pass along to the task sub-processes.


class NoopPackageInstallerProvider(PackageInstallerProvider):
  def from_task(self, task, sandbox):
    return NoopPackageInstaller()
