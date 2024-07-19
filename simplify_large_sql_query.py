import sqlparse
import re

def format_string(statement, keep_whitespace=True):
    """
    Format the SQL statement to standardize casing and remove unnecessary whitespace.
    """
    statement = sqlparse.format(
        statement,
        keyword_case='upper',
        identifier_case='upper',
        strip_comments=True,
        reindent=True,
        strip_semicolon=True
    )

    if not keep_whitespace:
        statement = re.sub(r'\s+', ' ', statement).strip()
    return statement

def extract_subqueries(query):
    """
    Extracts subqueries from the main SQL query and returns a dictionary of the main query and subqueries.
    """
    query_formatted = format_string(query, keep_whitespace=False)

    query_formatted = query_formatted.replace("/  +/g", " ")  # remove double spaces
    query_formatted = query_formatted.replace("( ", "(")

    subqueries = {
        "main_query": query_formatted
    }
    
    alias_mapping = {}

    subqueries_list = ["main_query"]
    subquery_counter = 1

    while subqueries_list:
        alias_to_search = subqueries_list.pop(0)
        query_to_search = subqueries[alias_to_search]
        subquery_index = 0

        while subquery_index < len(query_to_search):
            if query_to_search[subquery_index] == "(":
                sub_query_start_index = subquery_index + 1

                # Check if next token is SELECT
                next_token = query_to_search[sub_query_start_index:].strip().split(" ")[0].upper()
                if next_token != "SELECT":
                    subquery_index += 1
                    continue

                sub_query_index = 0
                counter_closed_bracket_to_find = 1

                while True:
                    sub_query_index += 1
                    if sub_query_start_index + sub_query_index >= len(query_to_search):
                        break
                    if query_to_search[sub_query_start_index + sub_query_index] == "(":
                        counter_closed_bracket_to_find += 1
                    elif query_to_search[sub_query_start_index + sub_query_index] == ")":
                        counter_closed_bracket_to_find -= 1

                    if counter_closed_bracket_to_find == 0:
                        subquery_ = query_to_search[sub_query_start_index:sub_query_start_index + sub_query_index]
                        alias_potential_start_index = sub_query_start_index + sub_query_index + 1
                        potential_aliases = query_to_search[alias_potential_start_index:].strip().split(" ")[:2]
                        alias = potential_aliases[1] if potential_aliases[0].upper() == "AS" else potential_aliases[0]

                        subquery_key = f"SUBQ{subquery_counter}"
                        subqueries[subquery_key] = subquery_
                        alias_mapping[subquery_key] = alias
                        subqueries_list.append(subquery_key)
                        subqueries[alias_to_search] = query_to_search.replace(f"({subquery_})", f"{{{subquery_key}}}", 1)

                        subquery_counter += 1
                        break

            subquery_index += 1

    return subqueries, alias_mapping

# Test cases
test_case1 = full_sql

# Example usage
parsed_queries1, alias_mapping1 = extract_subqueries(test_case1)

# Print the resulting dictionary
print("Parsed Queries for Test Case 1:")
for key, value in parsed_queries1.items():
    print(f'"{key}"')
print("Alias Mapping for Test Case 1:")
for key, value in alias_mapping1.items():
    print(f'"{key}": {value}')
