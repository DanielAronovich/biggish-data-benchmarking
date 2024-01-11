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

    # Convert 'p_partkey' to string in all relevant DataFrames
    part_df['p_partkey'] = part_df['p_partkey'].astype(str)
    partsupp_df['ps_partkey'] = partsupp_df['ps_partkey'].astype(str)


    # Convert 's_suppkey' in both supplier_df and partsupp_df to string
    supplier_df['s_suppkey'] = supplier_df['s_suppkey'].astype(str)
    partsupp_df['ps_suppkey'] = partsupp_df['ps_suppkey'].astype(str)
    part_df['p_type'] = part_df['p_type'].astype(str)

    # First merge: part_df with partsupp_df
    part_partsupp_merge = part_df.merge(partsupp_df, left_on='p_partkey', right_on='ps_partkey', how='inner')
    print("First merge successful, shape:", part_partsupp_merge.shape)

    # Second merge: Merge result with supplier_df
    part_partsupp_supplier_merge = part_partsupp_merge.merge(supplier_df, left_on='ps_suppkey', right_on='s_suppkey', how='inner')
    print("Second merge successful, shape:", part_partsupp_supplier_merge.shape)

    # Third merge: Merge result with nation_df
    part_partsupp_supplier_nation_merge = part_partsupp_supplier_merge.merge(nation_df, on='n_nationkey', how='inner')
    print("Third merge successful, shape:", part_partsupp_supplier_nation_merge.shape)

    # Fourth merge: Merge result with region_df
    joined_df = part_partsupp_supplier_nation_merge.merge(region_df, on='r_regionkey', how='inner')
    print("Fourth merge successful, shape:", joined_df.shape)


    # Applying the filter and query logic as per your SQL query
    # Filters
    filtered_df = joined_df[(joined_df['p_size'] == 15) &
                            (joined_df['p_type'].str.contains('BRASS')) &
                            (joined_df['r_name'] == 'EUROPE')]

    # Subquery for minimum ps_supplycost
    min_supplycost = (partsupp_df.merge(supplier_df, on='s_suppkey')
                                  .merge(nation_df, on='n_nationkey')
                                  .merge(region_df, on='r_regionkey')
                                  .query("r_name == 'EUROPE'")
                                  .groupby('ps_partkey')['ps_supplycost'].min())

    # Final query result
    result = filtered_df[filtered_df['ps_supplycost'].isin(min_supplycost)]

    # Sorting and limiting the results
    result = result.sort_values(by=['s_acctbal', 'n_name', 's_name', 'p_partkey'], ascending=[False, True, True, True]).head(100)

    return result