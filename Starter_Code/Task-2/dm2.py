# # import os
# # import time
# # import ray
# # import modin.pandas as pd
# # from datetime import datetime

# # # This function runs the data manipulation task with a specified number of CPUs
# # def run_data_manipulation_task(cpus, path):
# #     # Configure Ray to use the specified number of CPUs
# #     if ray.is_initialized():
# #         ray.shutdown()
# #     ray.init(num_cpus=cpus, ignore_reinit_error=True)
    
# #     # Record start time
# #     start_time = time.time()
    
# #     # Load the dataset and perform data manipulation
# #     raw_df1 = pd.read_csv(path)
# #     raw_df1['vote'] = pd.to_numeric(raw_df1['vote'], errors='coerce').fillna(0)
# #     raw_df=raw_df1[:100]
# #     grouped = raw_df.groupby('reviewerID').agg(
# #         num_product_reviewed=pd.NamedAgg(column='reviewerID', aggfunc='size'),
# #         mean_rating=pd.NamedAgg(column='overall', aggfunc='mean'),
# #         latest_review_year=pd.NamedAgg(column='unixReviewTime', aggfunc=lambda x: datetime.utcfromtimestamp(max(x)).year),
# #         num_helpful_votes=pd.NamedAgg(column='vote', aggfunc='sum')
# #     ).reset_index()
    
# #     # Fill NaN values in 'num_helpful_votes' with 0
# #     grouped['num_helpful_votes'] = grouped['num_helpful_votes'].fillna(0)
    
# #     # Record end time and calculate execution time
# #     end_time = time.time()
# #     execution_time = end_time - start_time
# #     print(f"Execution time with {cpus} CPU(s): {execution_time:.2f} seconds")
    
# #     # Shutdown Ray to release resources
# #     ray.shutdown()
    
# #     return execution_time

# # # Replace this with the path to your dataset
# # dataset_path = "amazon_reviews.csv"

# # # Number of CPUs configurations to test
# # cpu_configs = [1, 2, 3, 4]

# # # Run the data manipulation task for each CPU configuration and store the execution times
# # execution_times = {}
# # for cpus in cpu_configs:
# #     execution_times[cpus] = run_data_manipulation_task(cpus, dataset_path)

# # # Print the collected execution times
# # print("Execution times for different CPU counts:")
# # for cpus, time in execution_times.items():
# #     print(f"{cpus} CPU(s): {time:.2f} seconds")


# import os
# import time
# import ray
# import modin.pandas as pd
# from datetime import datetime

# # Ensure Modin uses Ray
# os.environ["MODIN_ENGINE"] = "ray"

# def load_and_process_data(path, num_cpus):
#     # Initialize Ray with the specified number of CPUs
#     if ray.is_initialized():
#         ray.shutdown()
#     ray.init(num_cpus=num_cpus, ignore_reinit_error=True)

#     # Record start time
#     start_time = time.time()

#     # Load the dataset
#     raw_df1 = pd.read_csv(path)
#     raw_df1['vote'] = pd.to_numeric(raw_df1['vote'], errors='coerce').fillna(0)
#     raw_df=raw_df1[:100]

#     # Example data manipulation task (can be replaced with your actual task)
#     # Here we just do a simple aggregation
#     grouped = raw_df.groupby('reviewerID').agg(
#         num_product_reviewed=pd.NamedAgg(column='reviewerID', aggfunc='size'),
#         mean_rating=pd.NamedAgg(column='overall', aggfunc='mean'),
#         latest_review_year=pd.NamedAgg(column='unixReviewTime', aggfunc=lambda x: datetime.utcfromtimestamp(max(x)).year),
#         num_helpful_votes=pd.NamedAgg(column='vote', aggfunc='sum')
#     ).reset_index()
#     grouped['num_helpful_votes'] = grouped['num_helpful_votes'].fillna(0)

#     # Record end time and calculate execution time
#     end_time = time.time()
#     execution_time = end_time - start_time

#     # Shutdown Ray
#     ray.shutdown()

#     return execution_time

# # Path to your dataset
# dataset_path = "amazon_reviews.csv"

# # CPU configurations to test
# cpu_configs = [1, 2, 3, 4]

# # Execute the task with different CPU counts and document execution times
# execution_times = {}
# for num_cpus in cpu_configs:
#     execution_time = load_and_process_data(dataset_path, num_cpus)
#     execution_times[num_cpus] = execution_time
#     print(f"Execution time with {num_cpus} CPU(s): {execution_time:.2f} seconds")

# # Discuss the execution times and speedup
# print("\nExecution times for different CPU counts:")
# for cpus, time in execution_times.items():
#     print(f"{cpus} CPU(s): {time:.2f} seconds")

# # Add your analysis on linear speedup based on the observed execution times



# import os
# import time
# import ray
# import modin.pandas as pd
# from datetime import datetime

# # Function to initialize Ray and run the data manipulation task
# def run_task(dataset_path, num_cpus):
#     # Ensure Ray uses the specified number of CPUs
#     if ray.is_initialized():
#         ray.shutdown()
#     ray.init(num_cpus=num_cpus)

#     # Start measuring time
#     start_time = time.time()

#     # Load the dataset
#     raw_df = pd.read_csv(dataset_path)

#     # Perform a sample data manipulation operation
#     # Replace this with your actual data manipulation logic
#     grouped = raw_df.groupby('reviewerID').agg(
#         num_products_reviewed=pd.NamedAgg(column='reviewerID', aggfunc='count'),
#         mean_rating=pd.NamedAgg(column='overall', aggfunc='mean'),
#         latest_review_year=pd.NamedAgg(column='unixReviewTime', aggfunc=lambda x: datetime.utcfromtimestamp(max(x)).year)
#     ).reset_index()

#     # End measuring time
#     end_time = time.time()
#     execution_time = end_time - start_time

#     # Cleanup Ray
#     ray.shutdown()

#     return execution_time

# # Example usage
# if __name__ == "__main__":
#     # Define the path to your dataset
#     dataset_path = "amazon_reviews.csv"

#     # Define the number of CPUs to use - you can modify this manually for each run,
#     # or implement a way to pass it as an argument or environment variable
#     num_cpus = 1  # Example: change this to 1, 2, 3, or 4 for different runs

#     # Run the task
#     execution_time = run_task(dataset_path, num_cpus)
#     print(f"Execution time with {num_cpus} CPUs: {execution_time:.2f} seconds")




import json
import os
import time
import ray
import modin.pandas as pd
from datetime import datetime

# Define a function to perform the data manipulation task
def run_data_manipulation_task(dataset_path, num_cpus):
    # Initialize Ray
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=num_cpus)

    start_time = time.time()

    # Load and process the dataset
    raw_df = pd.read_csv(dataset_path)
    def numeric_conversion(value):
        try:
            return float(value)
        except ValueError:
            return float(value.replace(',', ''))

    # Handling NaNs,string, float and converting  values to integers
    raw_df['vote'] = raw_df['vote'].apply(numeric_conversion)
    raw_df['vote'].fillna(0, inplace=True)
    raw_df['vote'] = raw_df['vote'].astype(int)

    raw_df['reviewYear'] = pd.to_datetime(raw_df['unixReviewTime'], unit='s').dt.year

    num_product_reviewed = raw_df.groupby('reviewerID').size()
    mean_rating = raw_df.groupby('reviewerID')['overall'].mean()
    latest_review_year = raw_df.groupby('reviewerID')['reviewYear'].max()
    num_helpful_votes = raw_df.groupby('reviewerID')['vote'].sum()

    all_stats = {
        'num_products_reviewed': num_product_reviewed.describe().round(2).to_dict(),
        'mean_rating': mean_rating.describe().round(2).to_dict(),
        'latest_review_year': latest_review_year.describe().round(2).to_dict(),
        'num_helpful_votes': num_helpful_votes.describe().round(2).to_dict()
    }

    

    # Insert your data manipulation logic here
    
    end_time = time.time()
    execution_time = end_time - start_time

    # Shutdown Ray
    ray.shutdown()

    return execution_time

if __name__ == "__main__":
    dataset_path = "amazon_reviews.csv"  # Update this path
    cpu_configs = [1, 2, 3, 4]  # Different CPU configurations to test
    execution_times = {}

    num_cpus=4
    execution_time = run_data_manipulation_task(dataset_path, num_cpus)
    execution_times[num_cpus] = execution_time
    print("done once, ",execution_times[num_cpus])
    print(f"Execution time with {num_cpus} CPUs: {execution_time:.2f} seconds")



    json_file_path = 'execution_times1.json'
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    data[num_cpus]=execution_times[num_cpus]
    with open(json_file_path, 'w') as file:
        json.dump(data, file, indent=4)


    # # Write the execution times to a JSON file
    # with open('execution_times1.json', 'w') as f:
    #     json.dump(execution_times, f, indent=4)




    
# # Path to your JSON file
# json_file_path = 'path/to/your/file.json'

# # Step 1: Read the existing data from the file


# # Step 2: Modify the dictionary
# # Example: Adding a new key-value pair or updating an existing one
# data['new_key'] = 'New value'

# # You can also add more complex data like another dictionary or a list
# data['another_new_key'] = {'sub_key': 'sub_value'}

# # Step 3: Write the modified dictionary back to the file
# with open(json_file_path, 'w') as file:
#     json.dump(data, file, indent=4)

    print("Execution times saved to execution_times.json.")
