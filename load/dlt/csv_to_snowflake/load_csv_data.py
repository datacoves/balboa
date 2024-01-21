import dlt
import pandas as pd
from datacoves_snowflake import db_config

if __name__ == "__main__":

    datacoves_snowflake = dlt.destinations.snowflake(
        db_config,
        destination_name="datacoves_snowflake"
    )

    us_population_csv = "https://raw.githubusercontent.com/dataprofessor/dashboard-v3/master/data/us-population-2010-2019.csv"
    df = pd.read_csv(us_population_csv)
    data = df.to_dict(orient="records")

    # dataset_name is the target schema name
    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination=datacoves_snowflake,
        dataset_name="us_population",
        progress="enlighten"
    )

    load_info = pipeline.run(
        data,
        table_name="us_population",
        write_disposition="replace"
    )

    print(load_info)

