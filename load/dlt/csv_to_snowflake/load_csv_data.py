# import dlt
# import pandas as pd
# from datacoves_snowflake import db_config

# if __name__ == "__main__":

#     datacoves_snowflake = dlt.destinations.snowflake(
#         db_config,
#         destination_name="datacoves_snowflake"
#     )

#     us_population_csv = "https://raw.githubusercontent.com/dataprofessor/dashboard-v3/master/data/us-population-2010-2019.csv"
#     df = pd.read_csv(us_population_csv)
#     data = df.to_dict(orient="records")

#     # dataset_name is the target schema name
#     pipeline = dlt.pipeline(
#         pipeline_name="from_csv",
#         destination=datacoves_snowflake,
#         dataset_name="us_population",
#         progress="enlighten"
#     )

#     load_info = pipeline.run(
#         data,
#         table_name="us_population",
#         write_disposition="replace"
#     )

#     print(load_info)



import dlt
import pandas as pd
from datacoves_snowflake import db_config

# a resource is the individual endpoints or tables
@dlt.resource(write_disposition="replace")
# method name = table name
def us_population():
    us_population_csv = "https://raw.githubusercontent.com/dataprofessor/dashboard-v3/master/data/us-population-2010-2019.csv"
    df = pd.read_csv(us_population_csv)
    yield df

# Source (corresponds to API or database)
@dlt.source
def us_population_source():
    return [us_population]

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
        db_config,
        destination_name="datacoves_snowflake"
    )

    pipeline = dlt.pipeline(
        progress = "enlighten",
        pipeline_name = "csv_to_snowflake",
        destination = datacoves_snowflake,
        pipelines_dir = "/tmp/",

        # dataset_name is the target schema name
        dataset_name="us_population"
    )

    load_info = pipeline.run(us_population())

    print(load_info)
