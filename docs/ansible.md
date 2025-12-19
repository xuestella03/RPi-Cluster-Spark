# Running in Ansible
*This doc includes information on the setup and how to run benchmarks using Ansible*

## Setting up to Run
**Update config and other variables**
If using venv, don't forget to start it :) `source .venv/bin/activate`

**Pick queries**

*This should eventually become CLI args*

## Running Benchmarks
After setting up, run the following:

```bash
# start cluster (this will display the link to the Spark dashboard)
ansible-playbook -i ansible/inventory/hosts.yml ansible/playbooks/start-cluster.yml --ask-become-pass

# if ssh password is enabled, add 
--ask-pass 

# begin benchmark
ansible-playbook -i ansible/inventory/hosts.yml ansible/playbooks/run-tpch.yml 

# stop cluster
ansible-playbook -i ansible/inventory/hosts.yml ansible/playbooks/stop-cluster.yml --ask-become-pass
```