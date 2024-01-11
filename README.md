# biggish-data-benchmarking
A benchmarking project to compare the performance and usability of various data processing tools like Pandas, DuckDB, SQLite, Dask, Modin, and Vaex on a single laptop


## A. Getting started





### B. Generate input data tables

Navigate to the data generator directory `dbgen` and build the data generator:

```
cd dbgen
make
```

For different size tables you can use the `-s` (scale) option. For example for 1GB,

```
./dbgen -s 1
```

Note that by default, `dbgen` uses a `|` as a column separator, and includes a `|` at the end of each entry.

Then run the python code that uses pandas and pyarrow to convert the csv files to parquet (with the scale factor 1, adjust it according the needed scale factor): 
```
cd..
python convert_to_parquet.py 1 
```

This will creat a folder named tables_scale_X at the root folder of the project.