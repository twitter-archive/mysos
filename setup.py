import os
from setuptools import setup, find_packages


COMMONS_VERSION = '==0.3.2'
MESOS_VERSION = '==0.20.1'


here = os.path.abspath(os.path.dirname(__file__))


def make_commons_requirement(name):
  return 'twitter.common.{0}{1}'.format(name, COMMONS_VERSION)


def list_package_data_files(package_root, data_folder):
  """List the data files in the data_folder under the given package_root."""
  paths = []
  for root, _, files in os.walk(os.path.join(package_root, data_folder)):
    for filename in files:
      paths.append(os.path.relpath(os.path.join(root, filename), package_root))

  return paths


setup(
    name='mysos',
    version='0.1.0-dev0',
    description='Mysos (MySQL on Mesos)',
    url='https://github.com/twitter/mysos',
    license='Apache License, Version 2.0',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='mesos mysql',
    packages=find_packages(exclude=['tests*']),
    package_data={
        '': (list_package_data_files('mysos/executor', 'files') +
             list_package_data_files('mysos/scheduler', 'assets'))
    },
    install_requires=[
        'kazoo==1.3.1',
        'mako==0.4.0',
        'mesos.interface{0}'.format(MESOS_VERSION),
        'pyyaml==3.10',
        make_commons_requirement('app'),
        make_commons_requirement('collections'),
        make_commons_requirement('concurrent'),
        make_commons_requirement('exceptions'),
        make_commons_requirement('http'),
        make_commons_requirement('lang'),
        make_commons_requirement('log'),
        make_commons_requirement('quantity'),
        make_commons_requirement('zookeeper'),
    ],
    extras_require={
        'test': [
            'pynacl>=0.3.0',
            'webtest',
            'zake==0.2.1',
        ],
        'scheduler': [
            'cherrypy==3.2.2',
            'mesos.native{0}'.format(MESOS_VERSION),
            'pynacl>=0.3.0,<1',
        ],
        'executor': [
            'mesos.native{0}'.format(MESOS_VERSION),
        ],
        'test_client': [
            'sqlalchemy',
            'mysql-python'
        ]
    },
    entry_points={
        'console_scripts': [
            'mysos_scheduler=mysos.scheduler.mysos_scheduler:proxy_main [scheduler]',
            'mysos_executor=mysos.executor.mysos_executor:proxy_main [executor]',
            'vagrant_mysos_executor=mysos.executor.testing.vagrant_mysos_executor:proxy_main [executor]',
            'mysos_test_client=mysos.testing.mysos_test_client:proxy_main [test_client]',
        ],
    },
)
