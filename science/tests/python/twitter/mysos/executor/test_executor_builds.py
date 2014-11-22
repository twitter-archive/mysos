from twitter.mysos.common.testing import build_and_execute_pex_target


def test_testing_mysos_executor_build():
  build_and_execute_pex_target('tests/python/twitter/mysos/executor:testing_mysos_executor',
                               'dist/testing_mysos_executor.pex')


def test_mysos_executor_build():
  build_and_execute_pex_target('src/python/twitter/mysos/executor:mysos_executor',
                               'dist/mysos_executor.pex')
