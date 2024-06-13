from sqlalchemy import create_engine,text

# Define the location of your database
DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

# Create the engine to connect to the SQLite database
engine = create_engine(DATABASE_LOCATION)
def drop_table(table_name):
	with engine.connect() as connection:
		drop_query = text(f"DROP TABLE IF EXISTS {table_name};")
		connection.execute(drop_query)
		print(f"Table {table_name} has been dropped.")

if __name__ == "__main__":
	drop_table("my_played_tracks")
	drop_table("fav_artist")
