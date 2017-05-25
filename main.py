from src.redis_client import RedisClient
from src.helper import data_to_dict_customer,data_to_dict
import psycopg2
import time

connPostgres= psycopg2.connect(
            dbname = 'carolina_zamith',
            host= 'localhost',
            port= '5432')

cur = connPostgres.cursor()

#cur.execute('select c_custkey, c_name, c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment from dev.customer')
#cur.execute('select l_orderkey, l_partkey, l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment from dev.lineitem')
#cur.execute('select n_nationkey,n_name,n_regionkey from dev.nation')
#cur.execute('select o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment from dev.orders')
#cur.execute('select p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment from dev.part')
#cur.execute('select ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment from dev.partsupp')
#cur.execute('select r_regionkey,r_name,r_comment from dev.region')
cur.execute('select s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment from dev.supplier')

RELATION = 'supplier'

start = time.clock()
row = cur.fetchall()
redis_client = RedisClient()

for r in row:
       data_dict = data_to_dict(r,RELATION)
       redis_client.store_dataset(data_dict)
       redis_client.execute()


end = time.clock()

elapsed_time = end-start
print(elapsed_time)
print('-----------------------------------------')
print(redis_client.get_dbsize())
