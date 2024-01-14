import pandas as pd
import os

current_dir = os.path.dirname(__file__)
data_dir = 'tables_scale_1'

def query1():
    lineitem_file = os.path.join(current_dir, '..', data_dir, 'lineitem.parquet')
    lineitem_df = pd.read_parquet(lineitem_file)

    lineitem_df['l_shipdate'] = pd.to_datetime(lineitem_df['l_shipdate'], errors='coerce')
    lineitem_df['l_discount'] = pd.to_numeric(lineitem_df['l_discount'], errors='coerce').astype('float64')
    lineitem_df['l_tax'] = pd.to_numeric(lineitem_df['l_tax'], errors='coerce').astype('float64')

    # Apply the query logic
    result = (lineitem_df[lineitem_df['l_shipdate'] <= pd.Timestamp('1998-09-02')]
              .groupby(['l_returnflag', 'l_linestatus'])
              .agg(sum_qty=('l_quantity', 'sum'),
                   sum_base_price=('l_extendedprice', 'sum'),
                   sum_disc_price=('l_extendedprice', lambda x: (x * (1 - lineitem_df.loc[x.index, 'l_discount'])).sum()),
                   sum_charge=('l_extendedprice', lambda x: (x * (1 - lineitem_df.loc[x.index, 'l_discount']) * (1 + lineitem_df.loc[x.index, 'l_tax'])).sum()),
                   avg_qty=('l_quantity', 'mean'),
                   avg_price=('l_extendedprice', 'mean'),
                   avg_disc=('l_discount', 'mean'),
                   count_order=('l_orderkey', 'count'))
              .sort_values(by=['l_returnflag', 'l_linestatus']))

    return result.head(100)




def query2():


    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    current_dir = os.path.dirname(__file__)

    # Define the paths to the Parquet files
    part_file = os.path.join(current_dir, '..', 'tables_scale_1', 'part.parquet')
    partsupp_file = os.path.join(current_dir, '..', 'tables_scale_1', 'partsupp.parquet')
    supplier_file = os.path.join(current_dir, '..', 'tables_scale_1', 'supplier.parquet')
    nation_file = os.path.join(current_dir, '..', 'tables_scale_1', 'nation.parquet')
    region_file = os.path.join(current_dir, '..', 'tables_scale_1', 'region.parquet')

    # Load the data from Parquet files
    part_df = pd.read_parquet(part_file)
    partsupp_df = pd.read_parquet(partsupp_file)
    supplier_df = pd.read_parquet(supplier_file)
    nation_df = pd.read_parquet(nation_file)
    region_df = pd.read_parquet(region_file)

    # Perform the first merge
    europe = region_df[region_df['r_name'] == var3]
    europe = europe.merge(nation_df, left_on='r_regionkey', right_on='n_regionkey', how='inner')
    europe = europe.merge(supplier_df, left_on='n_nationkey', right_on='s_nationkey')
    europe = europe.merge(partsupp_df, left_on='s_suppkey', right_on='ps_suppkey')


    # Filtering part DataFrame and joining
    brass = part_df[(part_df['p_size'] == var1) & (part_df['p_type'].str.endswith(var2))]
    brass = brass.merge(europe, left_on='p_partkey', right_on='ps_partkey')

    # Grouping by and calculating minimum supply cost
    min_cost = brass.groupby('ps_partkey')['ps_supplycost'].min().reset_index()
    min_cost.rename(columns={'ps_supplycost': 'min'}, inplace=True)

    # Final join, filter, select, sort, and limit
    result = brass.merge(min_cost, left_on='ps_partkey', right_on='ps_partkey')
    result = result[result['ps_supplycost'] == result['min']]
    result = result[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
    result = result.sort_values(by=['s_acctbal', 'n_name', 's_name', 'p_partkey'], ascending=[False, True, True, True])
    result = result.head(100)

    return result
