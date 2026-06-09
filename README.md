# RPi-Cluster-Spark

This is the orchestration and testing for my Master's Project.

Relevant files:
- `ansible/playbooks/run-tpch-scala.yml`: contains the script for running and testing Spark jobs.
- `tpch-scala/src/main/scala/TpchBenchmark.scala`: contains the queries for tests.
- `ansible/playbooks/find-r.yml`: contains the script for calibrating and calculating `r` values for operation aware scheduling. 

