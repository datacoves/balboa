import pandas as pd
import os

my_var = os.getenv("VAR1","NOT FOUND")

def print_sample_dataframe():

    # Creating a simple DataFrame
    data = {'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [25, 30, 35],
            'City': ['New York', 'San Francisco', 'Los Angeles']}

    df = pd.DataFrame(data)

    # Displaying the DataFrame
    print("DataFrame created using Pandas:")
    print(df)
    print("my_var")
    print(my_var)

print_sample_dataframe()
