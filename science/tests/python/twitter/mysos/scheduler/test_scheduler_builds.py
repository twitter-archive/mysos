import subprocess


def test_scheduler_build():
  assert subprocess.call(["./pants", 'goal', 'binary', 'src/python/twitter/mysos/scheduler:mysos_scheduler']) == 0

  p = subprocess.Popen(
      ['dist/mysos_scheduler.pex', "--help"], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
  out, err = p.communicate()
  assert p.returncode == 1
  assert out.startswith('Options'), 'Unexpected build output: %s' % out
