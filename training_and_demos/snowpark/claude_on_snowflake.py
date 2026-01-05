from snowflake.snowpark import Session
from datacoves_connection import get_connection_properties
from snowflake.cortex import Complete


# build connection to Snowflake
session = Session.builder.configs(get_connection_properties()).create()


stream = Complete(
 "claude-3-5-sonnet",
 "how do I provide info about my db?",
 session=session,
 stream=True)

for update in stream:
 print(update)
