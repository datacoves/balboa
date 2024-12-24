import pandas as pd
import os

# Retrieve environment variables
var1 = os.getenv('VAR1')


def print_sample_dataframe():

    # Creating a simple DataFrame
    data = {'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [25, 30, 35],
            'City': ['New York', 'San Francisco', 'Los Angeles']}

    df = pd.DataFrame(data)

    # Displaying the DataFrame
    print("DataFrame created using Pandas:")
    print(df)
    print(f"My passed variable is: {var1} ")

print_sample_dataframe()
