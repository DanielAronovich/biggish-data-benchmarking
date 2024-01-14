# biggish-data-benchmarking
A benchmarking project to compare the performance and usability of various data processing tools on a single machine:

1. Pandas 
2. DuckDB  
3. SQLite 
4. Dask
5. Modin
6. Vaex
7. Ray
8. Spark
9. cuDF


## A. Getting started

After creating a venv for the project run:

```
pip install -r requirements.txt
```


### B. Generate input data tables

Run the bash script that first generates the tpch data tables in csv format approx 1Gb in size (scale factor 1), then it converts it to parquet files
```
./populate_data.sh 1
```

This will creat a folder named tables_scale_1 at the root folder of the project. 
