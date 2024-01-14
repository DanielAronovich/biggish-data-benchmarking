import pandas as pd
import os
import sys






# Define the schema for each table using Pandas data types
schema_dict = {
    "customer": {
        "c_custkey": "Int64",
        "c_name": "string",
        "c_address": "string",
        "c_nationkey": "Int64",
        "c_phone": "string",
        "c_acctbal": "float64",
        "c_mktsegment": "string",
        "c_comment": "string"
    },
    "lineitem": {
        "l_orderkey": "Int64",
        "l_partkey": "Int64",
        "l_suppkey": "Int64",
        "l_linenumber": "Int64",
        "l_quantity": "float64",
        "l_extendedprice": "float64",
        "l_discount": "float64",
        "l_tax": "string",
        "l_returnflag": "string",
        "l_linestatus": "string",
        "l_shipdate": "string",
        "l_commitdate": "string",
        "l_receiptdate": "string",
        "l_shipinstruct": "string",
        "l_shipmode": "string",
        "l_comment": "string"
    },
    "nation": {
        "n_nationkey": "Int64",
        "n_name": "string",
        "n_regionkey": "Int64",
        "n_comment": "string"
    },
    "orders": {
        "o_orderkey": "Int64",
        "o_custkey": "Int64",
        "o_orderstatus": "string",
        "o_totalprice": "float64",
        "o_orderdate": "string",
        "o_orderpriority": "string",
        "o_clerk": "string",
        "o_shippriority": "Int64",
        "o_comment": "string"
    },
    "part": {
        "p_partkey": "Int64",
        "p_name": "string",
        "p_mfgr": "string",
        "p_brand": "string",
        "p_type": "string",
        "p_size": "Int64",
        "p_container": "string",
        "p_retailprice": "float64",
        "p_comment": "string"
    },
    "partsupp": {
        "ps_partkey": "Int64",
        "ps_suppkey": "Int64",
        "ps_availqty": "Int64",
        "ps_supplycost": "float64",
        "ps_comment": "string"
    },
    "region": {
        "r_regionkey": "Int64",
        "r_name": "string",
        "r_comment": "string"
    },
    "supplier": {
        "s_suppkey": "Int64",
        "s_name": "string",
        "s_address": "string",
        "s_nationkey": "Int64",
        "s_phone": "string",
        "s_acctbal": "float64",
        "s_comment": "string"
    }
}



# Retrieve scale factor from command line argument
scale_fac = int(sys.argv[1])

input_folder = "dbgen/"
output_folder = f"tables_scale_{scale_fac}/"

# Ensure the output directory exists
os.makedirs(output_folder, exist_ok=True)

for table_name, table_schema in schema_dict.items():
    print(f"Processing table: {table_name}")

    # Specify column names and their data types
    column_names = list(table_schema.keys())
    data_types = {col: dtype for col, dtype in table_schema.items()}

    # Read the CSV file with the specified column names and data types
    # The `on_bad_lines='skip'` parameter is used to skip any bad lines in the file
    # Use `dtype=str` to initially read all columns as strings to avoid initial parsing errors
    df = pd.read_csv(f"{input_folder}{table_name}.tbl", sep="|", names=column_names + ["dummy"], header=None, dtype=str, on_bad_lines='skip')

    # Drop the extra dummy column caused by the trailing delimiter
    df = df.drop(columns=["dummy"])

    # Convert columns to their correct data types
    for col, dtype in data_types.items():
        df[col] = df[col].astype(dtype)

    # Write the DataFrame to Parquet format
    df.to_parquet(f"{output_folder}{table_name}.parquet", index=False)


print("All files have been processed.")