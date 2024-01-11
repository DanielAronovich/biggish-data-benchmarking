#!/bin/bash

# Check if a scale factor was provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <scale_factor>"
    exit 1
fi

# Assign the first argument as the scale factor
SCALE_FACTOR=$1

# Navigate to the dbgen directory
cd dbgen

# Build the data generator
make

# Run the dbgen program with the specified scale factor
./dbgen -s $SCALE_FACTOR

# Navigate back to the root directory
cd ..

# Run the Python script to convert CSV files to Parquet format
python convert_to_parquet.py $SCALE_FACTOR

# Print completion message
echo "Data generation and conversion completed."
