import subprocess


def build_and_execute_pex_target(target, binary):
  assert subprocess.call(["./pants", target]) == 0

  p = subprocess.Popen([binary, "--help"], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
  out, err = p.communicate()
  assert p.returncode == 1
  assert out.startswith('Options'), 'Unexpected build output: %s' % out


def test_testing_mysos_executor_build():
  build_and_execute_pex_target('tests/python/twitter/mysos/executor:testing_mysos_executor',
                               'dist/testing_mysos_executor.pex')
