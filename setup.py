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
    namespace_packages=['twitter'],
    package_data={
        '': (list_package_data_files('twitter/mysos/executor', 'files') +
             list_package_data_files('twitter/mysos/scheduler', 'assets'))
    },
    install_requires=[
        'cherrypy==3.2.2',
        'kazoo==1.3.1',
        'mako==0.4.0',
        'mesos.interface{0}'.format(MESOS_VERSION),
        'mesos.native{0}'.format(MESOS_VERSION),
        'mysql-python',
        'pyyaml==3.10',
        'sqlalchemy',
        'zake==0.2.1',
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
        'test': ['webtest',],
    },
    entry_points={
        'console_scripts': [
            'mysos_scheduler=twitter.mysos.scheduler.mysos_scheduler:proxy_main',
            'mysos_executor=twitter.mysos.executor.mysos_executor:proxy_main',
            'vagrant_mysos_executor=twitter.mysos.executor.testing.vagrant_mysos_executor:proxy_main',
            'fake_mysos_executor=twitter.mysos.executor.testing.fake_mysos_executor:proxy_main',
            'mysos_test_client=twitter.mysos.testing.mysos_test_client:proxy_main',
        ],
    },
)
