Install JDK 17 (stable build is Java 17)

```
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Version: 
- Scala 2.13.18
- Spark 4.2.0-SNAPSHOT

Follow instructions [here](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html) to install sbt.


## Building After Making Changes
```
# if edited spark
./build/mvn -DskipTests clean package -pl sql/core,core,launcher -am
./build/mvn -DskipTests package -pl assembly -am

# if edited spark or scala query
sbt package

```

## Running
```
# if changed spark
ansible-playbook ansible/playbooks/deploy-custom-spark.yml -i ansible/inventory/hosts.yml --ask-pass

# to run benchmark
ansible-playbook ansible/playbooks/run-tpch-scala.yml -i ansible/inventory/hosts.yml  --ask-become-pass --ask-pass 
```