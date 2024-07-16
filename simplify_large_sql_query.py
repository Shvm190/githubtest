from collections import deque

def identify_subqueries(query):
  """
  Identifies subqueries from the main query and creates a dictionary.

  Args:
      query: The SQL query as a multi-line string.

  Returns:
      A dictionary with the main query and subqueries as variables.
  """
  subqueries = {}
  stack = deque()
  current_alias = None
  result = ""

  for line in query.splitlines():
    line = line.strip()
    if not line:
      continue

    if line.lower().startswith("select"):
      # New subquery or main query
      if stack:
        # Subquery
        subquery_str = "".join(stack)
        subquery_alias = current_alias or f"SUBQ{len(subqueries) + 1}"
        subqueries[subquery_alias] = f"""{subquery_str}"""
        current_alias = None
        stack.clear()
      else:
        # Main query
        result += line

    elif line.lower().split()[0] in ("from", "join", "on", "where"):
      # Part of the current query (main or sub)
      if stack:
        stack.append(line + " ")
      else:
        result += line + " "

    else:
      # Potential alias
      parts = line.split()
      if len(parts) > 1 and parts[-2] == "as":
        current_alias = parts[-1]

    # Handle closing parenthesis
    if ")" in line:
      if not stack:
        raise ValueError("Unmatched closing parenthesis")
      stack.pop()

  if stack:
    raise ValueError("Unmatched opening parenthesis")

  # Wrap main query with subquery alias if needed
  if subqueries:
    main_query = f"{result} ({subqueries.popitem()[1]})"
  else:
    main_query = result

  subqueries["main_query"] = f"""{main_query}"""
  return subqueries

# Example usage
query = """
Select distinct a,b,c from (select a,y from db.df1 DF1_ALIAS left join (select * from db.df2) DF2_ALIAS DF2_ALIAS.x= DF1_ALIAS.x) SUBQ1 left join db.df3 DF3_NALIAS on SUBQ1.y == DF3_NALIAS.y
"""

subquery_dict = identify_subqueries(query)
print(subquery_dict)
