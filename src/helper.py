import psycopg2
import json
import base64



def data_to_file(self,redis_dict):

        file = 'redis_in_file.out'

        target = open(file,'w')
        for k,v in redis_dict.iteritems():
            #target.write("SET" + " "  + " " + str(redis_dict))
            target.write("SET" + " "  + k + " " + str(v))
            target.writelines("\n")

        target.close()

def data_to_dict(r,relation):

    if relation == 'customer':
        return data_to_dict_customer(r)
    if relation == 'lineitem':
        return data_to_dict_lineitem(r)
    if relation == 'nation':
        return data_to_dict_nation(r)
    if relation == 'orders':
        return data_to_dict_orders(r)
    if relation == 'part':
        return data_to_dict_part(r)
    if relation == 'partSupp':
        return data_to_dict_partsupp(r)
    if relation == 'region':
        return data_to_dict_region(r)
    if relation == 'supplier':
        return data_to_dict_supplier(r)

def data_to_dict_customer(row):
        key = str(row[0])+'_customer'
        data_dict = {
           key : {
                'c_name': row[1],
                'c_address': row[2],
                'c_nationkey': row[3],
                'c_phone': row[4],
                'c_acctbal': row[5],
                'c_mktsegment':row[6]

            }

        }
        return data_dict

def data_to_dict_lineitem(row):
        key = str(row[0])+'_lineitem'
        data_dict = {
           key : {

                'l_partkey':row[1],
                'l_suppkey':row[2],
                'l_linenumber':row[3],
                'l_quantity':row[4],
                'l_extendedprice':row[5],
                'l_discount':row[6],
                'l_tax':row[7],
                'l_returnflag':row[8],
                'l_linestatus':row[9],
                'l_shipdate':row[10],
                'l_commitdate':row[11],
                'l_receiptdate':row[12],
                'l_shipinstruct':row[13],
                'l_shipmode':row[14],
                'l_comment':row[15]

            }

        }
        return data_dict

def data_to_dict_nation(row):
        key = str(row[0])+'_nation'
        data_dict = {
           key : {

                'n_name':row[1],
                'n_regionkey':row[2],

            }

        }
        return data_dict


def data_to_dict_orders(row):
        key = str(row[0])+'_orders'
        data_dict = {
           key : {
                'o_custkey':row[1],
                'o_orderstatus':row[2],
                'o_totalprice':row[3],
                'o_orderdate':row[4],
                'o_orderpriority':row[5],
                'o_clerk':row[6],
                'o_shippriority':row[7],
                'o_comment':row[8]

            }

        }
        return data_dict

def data_to_dict_part(row):
        key = str(row[0])+'_part'
        data_dict = {
           key : {
                'p_name': row[1],
                'p_mfgr': row[2],
                'p_brand': row[3],
                'p_type': row[4],
                'p_size': row[5],
                'p_container':row[6],
                'p_retailprice':row[7],
                'p_comment':row[8]

            }

        }
        return data_dict

def data_to_dict_partsupp(row):
        key = str(row[0])+'_partsupp'
        data_dict = {
           key : {
                'ps_suppkey': row[1],
                'ps_availqty': row[2],
                'ps_supplycost': row[3],
                'ps_comment': row[4]
            }

        }
        return data_dict

def data_to_dict_region(row):
        key = str(row[0])+'_region'
        data_dict = {
           key : {
                'r_name': row[1],
                'r_comment': row[2]
              }

        }
        return data_dict

def data_to_dict_supplier(row):
        key = str(row[0])+'_supplier'
        data_dict = {
           key : {
                's_name': row[1],
                's_address': row[2],
                's_nationkey': row[3],
                's_phone': row[4],
                's_acctbal': row[5],
                's_comment':row[6]

            }

        }
        return data_dict