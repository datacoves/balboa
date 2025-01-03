#!/usr/bin/env python3

input_file = "/config/.dbt/profiles.yml.bak"
output_file = "/config/.dbt/profiles.yml"

# Open the output file in write mode to replace its content if it already exists
with open(input_file, "r") as infile, open(output_file, "w") as outfile:
    content = infile.read()
    outfile.write(content)  # Write the original content
    outfile.write("\n\n")  # Add a newline for separation
    modified_content = content.replace("default:", "elementary:")  # Replace top-level key
    outfile.write(modified_content)  # Append the modified content

print(f"Updated dbt profiles file to add elementary top level key at: {output_file}")
