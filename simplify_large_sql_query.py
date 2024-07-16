import re

def extract_subqueries(query):
    """
    Extracts subqueries from a SQL query string and returns a dictionary with subquery aliases as keys.
    
    Parameters:
    query (str): The SQL query string.

    Returns:
    dict: A dictionary with subquery aliases as keys and subquery strings as values.
    """
    subqueries = {}
    pattern = re.compile(r'\(([^()]+)\)\s*(\w+)', re.DOTALL)

    while True:
        matches = pattern.findall(query)
        if not matches:
            break
        for subquery, alias in matches:
            subqueries[alias] = subquery.strip()
            query = query.replace(f'({subquery}) {alias}', alias, 1)
    
    subqueries['main_query'] = query.strip()
    return subqueries

def format_subqueries(subqueries):
    """
    Formats the subqueries and main query in the dictionary as f-strings.

    Parameters:
    subqueries (dict): A dictionary with subquery aliases as keys and subquery strings as values.

    Returns:
    dict: A dictionary with formatted subquery strings.
    """
    formatted_subqueries = {}
    for key, value in subqueries.items():
        formatted_subqueries[key] = f'f"""{value}"""'
    return formatted_subqueries

def clean_query(query):
    """
    Removes newlines, tabs, and extra spaces from a SQL query string.

    Parameters:
    query (str): The SQL query string.

    Returns:
    str: The cleaned SQL query string.
    """
    return re.sub(r'\s+', ' ', query).strip()

def clean_queries_dict(queries_dict):
    """
    Cleans each query string in the dictionary by removing newlines, tabs, and extra spaces.

    Parameters:
    queries_dict (dict): A dictionary with subquery aliases as keys and subquery strings as values.

    Returns:
    dict: A dictionary with cleaned query strings.
    """
    cleaned_dict = {}
    for key, value in queries_dict.items():
        cleaned_value = clean_query(value)
        cleaned_dict[key] = cleaned_value
    return cleaned_dict

def parse_sql_query(query, keep_whitespace=False):
    """
    Parses a SQL query to extract subqueries, formats them, and optionally cleans the query strings.
    
    Parameters:
    query (str): The SQL query string.
    keep_whitespace (bool): Whether to keep newline and tab characters in the output. Default is False.

    Returns:
    dict: A dictionary with the main query and subqueries.
    """
    # Extract subqueries and aliases
    subqueries = extract_subqueries(query)
    
    # Format subqueries
    formatted_subqueries = format_subqueries(subqueries)
    
    if not keep_whitespace:
        # Clean subqueries by removing newlines and tabs
        cleaned_subqueries = clean_queries_dict(formatted_subqueries)
        return cleaned_subqueries
    
    return formatted_subqueries

# Example usage:
query = """
Select distinct a,b,c from (
    select a,y from db.df1 DF1_ALIAS 
    left join (select * from db.df2) DF2_ALIAS 
    on DF2_ALIAS.x = DF1_ALIAS.x
) SUBQ1 
left join db.df3 DF3_ALIAS 
on SUBQ1.y = DF3_ALIAS.y
"""

# Parse the query and get the cleaned dictionary
parsed_queries = parse_sql_query(query, keep_whitespace=False)

# Print the resulting dictionary
for key, value in parsed_queries.items():
    print(f'"{key}": {value},')
