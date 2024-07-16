import re

def extract_subqueries(query):
    subqueries = {}
    query_stack = []
    current_query = query
    alias_pattern = re.compile(r'\)\s*(\w+)')
    subquery_pattern = re.compile(r'\(([^()]+)\)')

    while True:
        match = subquery_pattern.search(current_query)
        if not match:
            break
        
        subquery = match.group(1)
        alias_match = alias_pattern.search(current_query, match.end())
        
        if alias_match:
            alias = alias_match.group(1)
            subqueries[alias] = f'({subquery})'
            current_query = current_query[:match.start()] + f'{alias}' + current_query[alias_match.end():]
        else:
            query_stack.append((match.start(), match.end()))
            current_query = current_query[:match.start()] + f'SUBQUERY_{len(query_stack)}' + current_query[match.end():]
    
    subqueries = {f'SUBQUERY_{i+1}': subquery for i, (_, subquery) in enumerate(query_stack)}
    subqueries['main_query'] = current_query

    return subqueries

def format_subqueries(subqueries):
    formatted_subqueries = {}
    for key, value in subqueries.items():
        formatted_subqueries[key] = f'f"""{value}"""'
    return formatted_subqueries

def main():
    query = """
    Select distinct a,b,c from (
        select a,y from db.df1 DF1_ALIAS 
        left join (select * from db.df2) DF2_ALIAS 
        on DF2_ALIAS.x = DF1_ALIAS.x
    ) SUBQ1 
    left join db.df3 DF3_ALIAS 
    on SUBQ1.y = DF3_ALIAS.y
    """
    
    subqueries = extract_subqueries(query)
    formatted_subqueries = format_subqueries(subqueries)
    
    for key, value in formatted_subqueries.items():
        print(f'"{key}": {value},')

if __name__ == "__main__":
    main()
