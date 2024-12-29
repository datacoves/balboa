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

def print_vars():
    print("#########")
    print(f"var1 is {os.getenv('VAR1')}")
    print(f"var2 is {os.getenv('VAR2')}")
    print(f"var3 is {os.getenv('VAR3')}")
    print("#########")

print_sample_dataframe()

print_vars()
