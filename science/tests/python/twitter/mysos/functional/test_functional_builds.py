from twitter.mysos.common.testing import build_and_execute_pex_target


def test_testing_client_build():
  build_and_execute_pex_target('tests/python/twitter/mysos/functional:mysos_test_client',
                               'dist/mysos_test_client.pex')
