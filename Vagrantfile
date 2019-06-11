# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  # CockroachDB Box
  config.vm.define "cockroach" do |conf|
    # Every Vagrant development environment requires a box. You can search for
    # boxes at https://atlas.hashicorp.com/search.
    conf.vm.box = "ubuntu/trusty64"

    config.vm.provider "virtualbox" do |vb|
        vb.memory = 2048
        vb.cpus = 2
    end

    # Fixes 'stdin: is not a tty' from being printed to stderr
    config.ssh.shell = "bash -c 'BASH_ENV=/etc/profile exec bash'"

    config.vm.provision "shell", inline: <<-SHELL
      printf "Downloading CockroachDB binary...  "
      wget --quiet https://binaries.cockroachdb.com/cockroach-latest.linux-amd64.tgz
      tar -xzf cockroach-latest.linux-amd64.tgz
      mv cockroach-latest.linux-amd64/cockroach /usr/local/bin
      rm -r cockroach-latest.linux-amd64.tgz cockroach-latest.linux-amd64
      printf "Done."

      hostname vagrant-cockroach
      mkdir /opt/cockroach

      echo 'session    required   pam_limits.so' >> /etc/pam.d/common-session
      echo 'session    required   pam_limits.so' >> /etc/pam.d/common-session-noninteractive
      echo '*              soft     nofile          35000' >> /etc/security/limits.conf
      echo '*              hard     nofile          35000' >> /etc/security/limits.conf
      echo 'root           soft     nofile          35000' >> /etc/security/limits.conf
      echo 'root           hard     nofile          35000' >> /etc/security/limits.conf
      printf 'Reloading vm to apply file descriptor limit changes...  '
    SHELL

    # Reboot machine to apply file descriptor limit adjustment
    config.vm.provision :reload

    # Always run provisioning steps to start the cluster; This makes it so that halting and restarting the box puts will start the cluster again
    config.vm.provision "shell", run: "always", inline: <<-SHELL
      pushd /opt/cockroach > /dev/null

      # Run a simple command to check if the database is up before starting the nodes
      if cockroach sql -e 'SHOW DATABASES;' 2>&1 > /dev/null | grep -q 'connection refused'
      then
        printf "Starting first node...  "
        cockroach start --store=cockroach-data-1 --insecure --background
        # Sleeping briefely prevents cockroach node start output from becoming interleaved
        sleep .2
        printf "Done.\n\n"

        printf "Starting second node and joining cluster"
        cockroach start --store=cockroach-data-2 --insecure --background --join=localhost:26257 --port=26258 --http-port=8081
        # Sleeping briefely prevents cockroach node start output from becoming interleaved
        sleep .2
        printf "Done.\n\n"

        printf "Starting third node and joining cluster"
        cockroach start --store=cockroach-data-3 --insecure --background --join=localhost:26257,localhost:26258 --port=26259 --http-port=8082
        # Sleeping briefely prevents cockroach node start output from becoming interleaved
        sleep .2
        printf "Done.\n\n"
      else
        printf "Nodes already running.\n"
      fi

      popd > /dev/null
    SHELL

    conf.vm.network :forwarded_port, guest: 26257, host: ENV['COCKROACH_SQL_PORT'] || 26257
    conf.vm.network :forwarded_port, guest: 8080, host: ENV['COCKROACH_HTTP_PORT'] || 8080
  end
end