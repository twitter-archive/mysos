from collections import defaultdict
import subprocess


class Fake(object):
  """
    A fake object that does nothing but recording the method calls and arguments.
  """

  def __init__(self):
    self.method_calls = defaultdict(list)

  def __getattr__(self, attr):
    def enqueue_arguments(*args, **kw):
      self.method_calls[attr].append((args, kw))
    return enqueue_arguments


def build_and_execute_pex_target(target, binary):
  """
    :param target: The pants target.
    :param binary: The path to the pex binary relative to the root of the repository.
  """
  assert subprocess.call(["./pants", "goal", "binary", target]) == 0

  p = subprocess.Popen([binary, "--help"], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
  out, err = p.communicate()
  assert p.returncode == 1
  assert out.startswith('Options'), 'Unexpected build output: %s' % out
