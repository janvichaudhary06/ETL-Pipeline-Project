import pandas as pd
import sqlite3

# Step 1: Extract
def extract(file_path):
    print("Extracting data...")
    return pd.read_csv(file_path)

# Step 2: Transform
def transform(df):
    print("Transforming data...")
    df.dropna(inplace=True)
    df['Average'] = df[['Math', 'Science', 'English']].mean(axis=1)
    return df

# Step 3: Load
def load(df, db_file):
    print("Loading data to database...")
    conn = sqlite3.connect(db_file)
    df.to_sql('student_grades', conn, if_exists='replace', index=False)
    conn.close()

# Run ETL
def run_etl():
    data = extract("data/grades.csv")
    transformed_data = transform(data)
    load(transformed_data, "output/grades.db")
    print("ETL process complete.")

if __name__ == "__main__":
    run_etl()
