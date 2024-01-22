import time
from pandas_queries import *
import sys


def run_query(query_func, query_num, output_dir, timing_file):

    start_time = time.time()

    print(f"Running panads query {query_num}...", end="")
    sys.stdout.flush()  # Ensure the message is displayed immediately

    # Execute the specific query logic and load the required data within the function
    result = query_func()

    # Calculate time taken
    elapsed_time = time.time() - start_time

    print(f" Completed in {elapsed_time:.2f} seconds.")
    sys.stdout.flush()

    # Save the results
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    result_file = os.path.join(output_dir, f'query{query_num}_output.csv')
    result.to_csv(result_file, index=False)

    # Record the timing result
    with open(timing_file, 'a') as f:
        if os.path.getsize(timing_file) == 0:
            f.write('Tool,Query,TimeInSeconds\n')
        f.write(f'Pandas,Query {query_num},{elapsed_time}\n')


if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    output_dir = os.path.join(current_dir, '..', 'pandas_analysis', 'pandas_output')
    timing_file = os.path.join(current_dir, '..', 'timing_results.csv')

    run_query(query1, 1, output_dir, timing_file)
    run_query(query2, 2, output_dir, timing_file)
    run_query(query3, 3, output_dir, timing_file)