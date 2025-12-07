'''
initialize the spark session
this is called by deploy-tpch.yml
  - load data using load_data.py
  - execute queries from queries.py
  - save results
'''

import pyspark
if __name__ == "__main__":
    print("testing...")
    print(pyspark.__version__)