import Extract
import pandas as pd
# Set of Data Quality Checks Needed to Perform Before Loading



def Data_Quality(load_df):
    # Checking whether the DataFrame is empty
	if load_df.empty:
		print('No songs extracted.')
		return False

    # Checking for duplicates in 'played_at' which is supposed to be unique
	if load_df['played_at'].duplicated().any():
		print("Duplicate entries found in 'played_at'.")
		return False

    # Checking for null values in any columns of the DataFrame
	if load_df.isnull().values.any():
		null_columns = load_df.columns[load_df.isnull().any()].tolist()
		print(f"Null values found in columns: {null_columns}")
		return False

	print("Data quality checks passed.")
	return True

def Transform_df(load_df):
	#Applying transformation logic
	Transformed_df=load_df.groupby(['timestamp','artist_name'],as_index = False).count()
	Transformed_df.rename(columns ={'played_at':'count'}, inplace=True)
	#Creating a Primary Key based on Timestamp and artist name
	Transformed_df["ID"] = Transformed_df['timestamp'].astype(str) +"-"+Transformed_df["artist_name"]
	return Transformed_df[['ID','timestamp','artist_name','count']]
#if __name__ == "__main__":
	#Importing the songs_df from the Extract.py
	#load_df=Extract.return_dataframe()
	#Data_Quality(load_df)
	#calling the transformation
	#Transformed_df=Transform_df(load_df)
	#print(Transformed_df)
	#print(load_df.head())
	#print(load_df.isnull().sum())  # Xem sá»‘ lÆ°á»£ng giĂ¡ trá»‹ Null cho má»—i cá»™t
	#print(load_df['played_at'].duplicated().any())  # Kiá»ƒm tra cĂ³ trĂ¹ng láº·p khĂ´ng
def main():
	load_df=Extract.return_dataframe()
	print(load_df.head())
	print(load_df.isnull().sum())  # Kiá»ƒm tra giĂ¡ trá»‹ null
	print(load_df['played_at'].duplicated().any())  # Kiá»ƒm tra trĂ¹ng láº·p

	if not Data_Quality(load_df):
		raise ValueError("Failed at Data Validation")
	else:
		print("Data Validation Passed")
	
	Transformed_df = Transform_df(load_df)
	print(Transformed_df.head())

if __name__ == "__main__":
	main()
