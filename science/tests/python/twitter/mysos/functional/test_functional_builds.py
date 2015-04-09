from twitter.mysos.common.testing import build_and_execute_pex_target


def test_testing_client_build():
  build_and_execute_pex_target('src/python/twitter/mysos/testing:mysos_test_client',
                               'dist/mysos_test_client.pex')


def test_twitter_restore_hdfs_build():
  build_and_execute_pex_target('src/python/twitter/mysos/executor/testing:twitter_restore_hdfs',
                               'dist/twitter_restore_hdfs.pex')


def test_mysos_runner_build():
  build_and_execute_pex_target('src/python/twitter/mysos/executor/testing:mysos_runner',
                               'dist/mysos_runner.pex')
