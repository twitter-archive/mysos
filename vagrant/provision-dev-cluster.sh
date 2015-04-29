#!/bin/bash -x

# This script requires mysos binaries to be built. Run `tox` first.

# Install dependencies.
export DEBIAN_FRONTEND=noninteractive
aptitude update -q
aptitude install -q -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" \
    libcurl3-dev \
    libsasl2-dev \
    python-dev \
    zookeeper \
    mysql-server-5.6 \
    libmysqlclient-dev \
    libunwind8 \
    python-virtualenv \
    bison flex  # For libnl.

# Fix up a dependency issue of Mesos egg: _mesos.so links to libunwind.so.7 but Trusty only has
# libunwind.so.8.
ln -sf /usr/lib/x86_64-linux-gnu/libunwind.so.8 /usr/lib/x86_64-linux-gnu/libunwind.so.7

# Fix up Ubuntu mysql-server-5.6 issue: mysql_install_db looks for this file even if we don't need
# it.
ln -sf /usr/share/doc/mysql-server-5.6/examples/my-default.cnf /usr/share/mysql/my-default.cnf

# Set the hostname to the IP address. This simplifies things for components that want to advertise
# the hostname to the user, or other components.
hostname 192.168.33.17

# Install an update-mysos tool to sync mysos binaries and configs into the VM.
cat > /usr/local/bin/update-mysos <<EOF
#!/bin/bash
mkdir -p /home/vagrant/mysos
rsync -urzvh /vagrant/vagrant/ /home/vagrant/mysos/vagrant/ --delete
rsync -urzvh /vagrant/.tox/dist/ /home/vagrant/mysos/dist/ --delete
rsync -urzvh /vagrant/3rdparty/ /home/vagrant/mysos/3rdparty/ --delete

# Install the upstart configurations.
sudo cp /home/vagrant/mysos/vagrant/upstart/*.conf /etc/init
chown -R vagrant:vagrant /home/vagrant/mysos
EOF
chmod +x /usr/local/bin/update-mysos
sudo -u vagrant update-mysos

# Install Mesos.
MESOS_VERSION=0.20.1
UBUNTU_YEAR=14
UBUNTU_MONTH=04

mkdir -p /home/vagrant/mysos/deps
pushd /home/vagrant/mysos/deps
if [ ! -f mesos_${MESOS_VERSION}-1.0.ubuntu${UBUNTU_YEAR}${UBUNTU_MONTH}_amd64.deb ]; then
    curl --silent --output mesos_${MESOS_VERSION}-1.0.ubuntu${UBUNTU_YEAR}${UBUNTU_MONTH}_amd64.deb \
        http://downloads.mesosphere.io/master/ubuntu/${UBUNTU_YEAR}.${UBUNTU_MONTH}/mesos_${MESOS_VERSION}-1.0.ubuntu${UBUNTU_YEAR}${UBUNTU_MONTH}_amd64.deb
    dpkg --install --force-confdef --force-confold \
        /home/vagrant/mysos/deps/mesos_${MESOS_VERSION}-1.0.ubuntu${UBUNTU_YEAR}${UBUNTU_MONTH}_amd64.deb
fi

if [ ! -f mesos.native-${MESOS_VERSION}-cp27-none-linux_x86_64.whl ]; then
    # NOTE: We rename the output file here so the egg is properly named.
    curl --silent --output mesos.native-${MESOS_VERSION}-py2.7-linux-x86_64.egg http://downloads.mesosphere.io/master/ubuntu/${UBUNTU_YEAR}.${UBUNTU_MONTH}/mesos-${MESOS_VERSION}-py2.7-linux-x86_64.egg
    python -m wheel convert mesos.native-${MESOS_VERSION}-py2.7-linux-x86_64.egg
fi
popd

# (Re)start services with new conf.
stop zookeeper ; start zookeeper
stop mesos-master ; start mesos-master
stop mesos-slave ; start mesos-slave
stop mysos ; start mysos
