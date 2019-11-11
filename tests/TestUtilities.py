import unittest
import nflapidb.Utilities as util

class TestUtilities(unittest.TestCase):
    
    def test_ddquery_1q_1md(self):
        q = [{"col1": "a"}]
        d = [{"col1": "a", "col2": 1}]
        self.assertEqual(util.ddquery(q, d), (d, [],))
    
    def test_ddquery_1q_1nmd(self):
        q = [{"col1": "a"}]
        d = [{"col1": "b", "col2": 1}]
        self.assertEqual(util.ddquery(q, d), ([], d,))
    
    def test_ddquery_1q_1md_1nmd(self):
        q = [{"col1": "a"}]
        d = [{"col1": "a", "col2": 1}, {"col1": "b", "col2": 1}]
        self.assertEqual(util.ddquery(q, d), ([d[0]], [d[1]],))
    
    def test_ddquery_2q_2md(self):
        q = [{"col1": "a"}, {"col1": "b"}]
        d = [{"col1": "a", "col2": 1}, {"col1": "b", "col2": 1}]
        self.assertEqual(util.ddquery(q, d), (d, [],))
    
    def test_ddquery_2q_2md_1nmd(self):
        q = [{"col1": "a"}, {"col1": "b"}]
        md = [{"col1": "a", "col2": 1}, {"col1": "b", "col2": 1}]
        mmd = [{"col1": "c", "col2": 2}]
        d = md + mmd
        self.assertEqual(util.ddquery(q, d), (md, mmd,))
    
    def test_ddquery_diff_q_keys_all(self):
        q = [{"col3": "a"}]
        d = [{"col1": "a", "col2": 1}]
        self.assertEqual(util.ddquery(q, d), ([], d,))
    
    def test_ddquery_diff_q_keys_some(self):
        q = [{"col1": "a", "col3": "a"}]
        d = [{"col1": "a", "col2": 1}]
        self.assertEqual(util.ddquery(q, d), ([], d,))