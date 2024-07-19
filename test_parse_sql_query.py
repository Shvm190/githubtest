import unittest

class TestExtractSubqueries(unittest.TestCase):
    def setUp(self):
        self.test_case1 = """
        Select distinct a,b,c from (select a,y, ROW_NUMBER() OVER (PARTITION BY DF2_ALIAS.Z ORDER BY CAST(SS.STATUS_SCORE AS INTEGER)) DESC from db.df1 DF1_ALIAS 
            left join (select * from db.df2) DF2_ALIAS 
            on DF2_ALIAS.x = DF1_ALIAS.x
        ) SUBQ1 
        left join db.df3 DF3_ALIAS 
        on SUBQ1.y = DF3_ALIAS.y
        """
        
        self.test_case2 = """
        select * from (select * from b) a
        """

    def test_extract_subqueries_case1(self):
        expected_queries = {
            "main_query": "SELECT DISTINCT A,B,C FROM {SUBQ1} SUBQ1 LEFT JOIN DB.DF3 DF3_ALIAS ON SUBQ1.Y = DF3_ALIAS.Y",
            "SUBQ1": "SELECT A,Y, ROW_NUMBER() OVER (PARTITION BY DF2_ALIAS.Z ORDER BY CAST(SS.STATUS_SCORE AS INTEGER)) DESC FROM DB.DF1 DF1_ALIAS LEFT JOIN {SUBQ2} DF2_ALIAS ON DF2_ALIAS.X = DF1_ALIAS.X",
            "SUBQ2": "SELECT * FROM DB.DF2"
        }
        
        expected_alias_mapping = {
            "SUBQ1": "SUBQ1",
            "SUBQ2": "DF2_ALIAS"
        }
        
        parsed_queries, alias_mapping = extract_subqueries(self.test_case1)
        
        self.assertEqual(parsed_queries, expected_queries)
        self.assertEqual(alias_mapping, expected_alias_mapping)

    def test_extract_subqueries_case2(self):
        expected_queries = {
            "main_query": "SELECT * FROM {SUBQ1} A",
            "SUBQ1": "SELECT * FROM B"
        }
        
        expected_alias_mapping = {
            "SUBQ1": "A"
        }
        
        parsed_queries, alias_mapping = extract_subqueries(self.test_case2)
        
        self.assertEqual(parsed_queries, expected_queries)
        self.assertEqual(alias_mapping, expected_alias_mapping)

if __name__ == "__main__":
    unittest.main()
