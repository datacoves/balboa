# run mysql -> snowflake pipeline using:
# python sql_database_pipeline.py

# Use this to delete target table
# dlt pipeline drop datacoves_tc2 --drop-all


import dlt
from sql_database import sql_table

if __name__ == "__main__":

    # dataset_name is the target schema name
    pipeline = dlt.pipeline(
        pipeline_name="datacoves_tc2",
        destination="snowflake",
        dataset_name="tc2",
        progress="enlighten")

    table = sql_table(table="tc2_lineitem")

    table.apply_hints(
        write_disposition="merge",
         primary_key=["L_ORDERKEY", "L_LINENUMBER"])

    print(pipeline.run(table))
