tajo-yarn
=========

# What is it?

Tajo-Yarn is a project helps launching a tajo cluster in a yarn cluster as well as adding/removing workers to/from that tajo cluster.

# Building

Run below command can build a tar ball for tajo-yarn

    > git clone https://github.com/coderplay/tajo-yarn.git
    > cd tajo-yarn
    > mvn -DskipTests package

# Getting started

Untar the binary tar ball

    > tar zxvf tajo-yarn-0.8.0-bin.tar.gz
    > cd tajo-yarn-0.8.0

Launch a tajo cluster with only TajoMaster started

    > bin/tajo-yarn launch -archive tajo-dist/tajo-0.8.0.tar.gz

You will get the `application id` for this empty cluster.

Add 3 Workers into the cluster

    > bin/tajo-yarn worker -appId the_application_id -add 3


# Help

    > bin/tajo-yarn help