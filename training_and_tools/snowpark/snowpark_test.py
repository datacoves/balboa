from snowflake.snowpark import Session
from datacoves_connection import get_connection_properties

# build connection to Snowflake
session = Session.builder.configs(get_connection_properties()).create()

# You can run SQL directly like:
session.sql("select current_warehouse() wh, current_database() db, current_schema() schema, current_version() ver").show()

df = session.table("BALBOA.L1_COUNTRY_DATA.COUNTRY_POPULATIONS")

# table profile
df.select(["country_code","country_name"]).distinct().describe().show()

rows = df.where("country_code like 'U%'").groupBy('country_code').count().limit(5)
rows.show()
