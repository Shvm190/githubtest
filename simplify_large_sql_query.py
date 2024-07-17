import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens import DML, Keyword

def is_subselect(parsed):
    """
    Checks if a parsed SQL component is a subquery by inspecting its structure.
    """
    if not isinstance(parsed, Parenthesis):
        return False
    for item in parsed.tokens:
        if isinstance(item, Identifier) and item.value.upper() == 'SELECT':
            return True
    return False

def extract_subqueries_from_statement(statement):
    """
    Extracts subqueries from a single SQL statement and returns a dictionary of subqueries.
    """
    subqueries = {}
    for item in statement.tokens:
        if isinstance(item, Parenthesis) and is_subselect(item):
            alias = None
            idx = statement.token_index(item)
            if idx > 0 and isinstance(statement.tokens[idx - 1], Identifier):
                alias = statement.tokens[idx - 1].value
            subquery = str(item)
            subqueries[alias] = subquery
    return subqueries

def extract_subqueries(query):
    """
    Extracts all subqueries from a SQL query and returns them as a dictionary.
    """
    parsed = sqlparse.parse(query)[0]  # Assuming single statement for simplicity
    subqueries = extract_subqueries_from_statement(parsed)
    subqueries['main_query'] = query.strip()
    return subqueries

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
parsed_queries = extract_subqueries(query)

# Print the resulting dictionary
for key, value in parsed_queries.items():
    print(f'"{key}": {value}')
