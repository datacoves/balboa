DEMO - Using datacoves/balboa (github.com/datacoves/balboa)

DEMO 1 - Show AGENTS.md:
PROMPT: "Explain how AGENTS.md file guides your behavior in this project."

DEMO 2 - Explore the project:
PROMPT: "Using what you know about this project from Agents.md, give me a quick overview of this project; layers, data domains, and key features. Be concise."

DEMO 3 - Load weather data with dlt skill (no Python needed):
PROMPT: "Use the dlt skill to load daily weather data from the Open-Meteo API for Singapore into Snowflake. Load temperature, humidity, and precipitation for the last 30 days. Once imported, Query the imported daily weather data for Singapore and show the the last 5 days."

DEMO 4 - Create complex dbt model with window functions:
PROMPT: "Create a new dbt model in L3_coves called weather_daily_stats that calculates: 7-day rolling average temperature, daily temperature rank within each month using RANK(), and day-over-day temperature change using LAG(). Follow the project's naming conventions from AGENTS.md. Once done, open the sql file and explain your query."

DEMO 5 - GitHub MCP:
PROMPT: "Create a GitHub issue on datacoves/balboa titled 'Add weather data mart with rolling averages' that describes the new weather pipeline we just built."
