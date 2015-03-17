from twitter.mysos.common.testing import build_and_execute_pex_target


def test_fake_mysos_executor_build():
  build_and_execute_pex_target('src/python/twitter/mysos/executor/testing:fake_mysos_executor',
                               'dist/fake_mysos_executor.pex')


def test_vagrant_mysos_executor_build():
  build_and_execute_pex_target('src/python/twitter/mysos/executor/testing:vagrant_mysos_executor',
                               'dist/vagrant_mysos_executor.pex')


def test_mysos_executor_build():
  build_and_execute_pex_target('src/python/twitter/mysos/executor:mysos_executor',
                               'dist/mysos_executor.pex')
