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
hostname 192.168.33.7

# Install an update-mysos tool to sync mysos binaries and configs into the VM.
cat > /usr/local/bin/update-mysos <<EOF
#!/bin/bash
mkdir -p /home/vagrant/mysos
rsync -urzvh /vagrant/vagrant/ /home/vagrant/mysos/vagrant/ --delete
rsync -urzvh /vagrant/.tox/dist/ /home/vagrant/mysos/dist/ --delete
rsync -urzvh /vagrant/3rdparty/ /home/vagrant/mysos/deps/ --delete
chown -R vagrant:vagrant /home/vagrant/mysos
EOF
chmod +x /usr/local/bin/update-mysos
sudo -u vagrant update-mysos

# Install Mesos.
MESOS_VERSION=0.20.1
UBUNTU_YEAR=14
UBUNTU_MONTH=04

pushd /home/vagrant/mysos/deps
if [ ! -f mesos_${MESOS_VERSION}-1.0.ubuntu${UBUNTU_YEAR}${UBUNTU_MONTH}_amd64.deb ]; then
    wget –quiet -c http://downloads.mesosphere.io/master/ubuntu/${UBUNTU_YEAR}.${UBUNTU_MONTH}/mesos_${MESOS_VERSION}-1.0.ubuntu${UBUNTU_YEAR}${UBUNTU_MONTH}_amd64.deb
    dpkg --install --force-confdef --force-confold \
        /home/vagrant/mysos/deps/mesos_${MESOS_VERSION}-1.0.ubuntu${UBUNTU_YEAR}${UBUNTU_MONTH}_amd64.deb
fi
popd

# Install the upstart configurations.
cp /home/vagrant/mysos/vagrant/upstart/*.conf /etc/init

# (Re)start services with new conf.
stop zookeeper ; start zookeeper
stop mesos-master ; start mesos-master
stop mesos-slave ; start mesos-slave
stop mysos ; start mysos
