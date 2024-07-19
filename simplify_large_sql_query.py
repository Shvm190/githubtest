import sqlparse
import re

# Test cases
test_case1 = """
Select distinct a,b,c from (select a,y, ROW_NUMBER() OVER (PARTITION BY DF2_ALIAS.Z ORDER BY CAST(SS.STATUS_SCORE AS INTEGER)) DESC from db.df1 DF1_ALIAS 
    left join (select * from db.df2) DF2_ALIAS 
    on DF2_ALIAS.x = DF1_ALIAS.x
) SUBQ1 
left join db.df3 DF3_ALIAS 
on SUBQ1.y = DF3_ALIAS.y
"""

test_case2 = """
select * from (select * from b) a
"""

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
    query_formatted = re.sub(r"\s+", " ", query_formatted)  # remove double spaces
    query_formatted = query_formatted.replace("( ", "(")

    subqueries = {
        "main_query": query_formatted
    }

    subqueries_list = ["main_query"]
    subquery_index = 0

    while subquery_index < len(subqueries_list):
        alias_to_search = subqueries_list[subquery_index]
        query_to_search_index = 0

        while query_to_search_index < len(subqueries[alias_to_search]):
            if subqueries[alias_to_search][query_to_search_index] == "(":
                sub_query_start_index = query_to_search_index + 1

                # Check if next token is SELECT
                next_token = subqueries[alias_to_search][sub_query_start_index:].strip().split(" ")[0].upper()
                if next_token != "SELECT":
                    query_to_search_index += 1
                    continue

                sub_query_index = 0
                counter_closed_bracket_to_find = 1

                while True:
                    sub_query_index += 1
                    if subqueries[alias_to_search][sub_query_start_index + sub_query_index] == "(":
                        counter_closed_bracket_to_find += 1
                    elif subqueries[alias_to_search][sub_query_start_index + sub_query_index] == ")":
                        counter_closed_bracket_to_find -= 1

                    if counter_closed_bracket_to_find == 0:
                        subquery_ = subqueries[alias_to_search][sub_query_start_index:sub_query_start_index + sub_query_index]
                        alias_potential_start_index = sub_query_start_index + sub_query_index + 1
                        potential_aliases = subqueries[alias_to_search][alias_potential_start_index:].strip().split(" ")[:2]
                        alias = potential_aliases[1] if potential_aliases[0].upper() == "AS" else potential_aliases[0]

                        # Add the extracted subquery to the dictionary
                        subqueries[alias] = subquery_

                        # Replace subquery in the main query with a placeholder
                        subqueries[alias_to_search] = subqueries[alias_to_search].replace(f"({subquery_})", f"{{{alias}}}", 1)
                        
                        subqueries_list.append(alias)
                        query_to_search_index = sub_query_start_index + sub_query_index - 1
                        break

            query_to_search_index += 1
        subquery_index += 1

    return subqueries

# Example usage:
parsed_queries1 = extract_subqueries(test_case1)
parsed_queries2 = extract_subqueries(test_case2)

# Print the resulting dictionary
print("Parsed Queries for Test Case 1:")
for key, value in parsed_queries1.items():
    print(f'"{key}": {value}')

print("\nParsed Queries for Test Case 2:")
for key, value in parsed_queries2.items():
    print(f'"{key}": {value}')
