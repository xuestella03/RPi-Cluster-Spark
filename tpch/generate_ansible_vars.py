import yaml
import config as tpch_config
import os

def generate_vars():
    vars_dict = {
        'tpch_query': tpch_config.CUR_QUERY,
        'tpch_config': tpch_config.CUR_CONFIG,
        'executor_memory': tpch_config.SPARK_EXECUTOR_MEMORY,
        'active_config': tpch_config.ACTIVE_CONFIG,
    }
    
    with open(f'{os.getcwd()}/ansible/inventory/group_vars/tpch_vars.yml', 'w') as f:
        yaml.dump(vars_dict, f)
    
    print(f"Generated vars: {vars_dict}")

if __name__ == "__main__":
    generate_vars()