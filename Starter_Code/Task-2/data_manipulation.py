import ray, json
# optional initialization here for ray
import modin.pandas as pd
from datetime import datetime

ray.init(ignore_reinit_error=True)
nodes = ray.nodes()
print(f"Number of nodes in Ray cluster: {len(nodes)}")

def run_task2(path):
    raw_df = pd.read_csv(path)



    # ## PLEASE COMPLETE THIS: START  


    # # shape of the dataframe
    # print(raw_df1.shape)
    # # headers of the dataframe 
    # print(raw_df1.head())
    # # column names
    # print(raw_df1.columns)
    # # data types of the columns
    # print(raw_df1.dtypes)
    # # number of non-null values for each column
    # print(raw_df1.count())

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

  
    # # ## PLEASE COMPLETE THIS: END
    # submit = output.describe().round(2)
    with open('results_PA0.json', 'w') as outfile: json.dump(all_stats, outfile)

if __name__ == "__main__":
    raw_dataset_path = "amazon_reviews.csv"  # PLEASE SET THIS

    run_task2(raw_dataset_path)



