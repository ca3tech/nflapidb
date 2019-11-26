import unittest
import inspect
from nflapidb.Entity import Entity, PrimaryKey, Column, Index

class TestPrimaryKey(unittest.TestCase):

    def test_def(self):
        @PrimaryKey
        def attr():
            return "str"

        self.assertTrue(inspect.isfunction(attr), "not a function")
        self.assertEqual(attr.__name__, "attr", "name mismatch")
        self.assertEqual(attr(), "str", "return value mismatch")
        self.assertTrue(hasattr(attr, "decorators"), "has no decorators attribute")
        self.assertEqual(getattr(attr, "decorators"), "PrimaryKey", "decorators attribute mismatch")

class TestColumn(unittest.TestCase):

    def test_def(self):
        @Column
        def attr():
            return "str"

        self.assertTrue(inspect.isfunction(attr), "not a function")
        self.assertEqual(attr.__name__, "attr", "name mismatch")
        self.assertEqual(attr(), "str", "return value mismatch")
        self.assertTrue(hasattr(attr, "decorators"), "has no decorators attribute")
        self.assertEqual(getattr(attr, "decorators"), "Column", "decorators attribute mismatch")

class TestIndex(unittest.TestCase):

    def test_def(self):
        @Index
        def attr():
            return "str"
        
        self.assertTrue(inspect.isfunction(attr), "not a function")
        self.assertEqual(attr.__name__, "attr", "name mismatch")
        self.assertEqual(attr(), "str", "return value mismatch")
        self.assertTrue(hasattr(attr, "decorators"), "has no decorators attribute")
        self.assertEqual(getattr(attr, "decorators"), "Index", "decorators attribute mismatch")

class TestEntity(unittest.TestCase):

    def test_primaryKey(self):
        class MyEntity(Entity):
            @PrimaryKey
            def attr1(self):
                return "str"
            @PrimaryKey
            def attr2(self):
                return "str"

        o = MyEntity()
        self.assertEqual(o.primaryKey, set(["attr1", "attr2"]))

    def test_indices(self):
        class MyEntity(Entity):
            @Index
            def attr1(self):
                return "str"
            @Index
            def attr2(self):
                return "str"

        o = MyEntity()
        self.assertEqual(o.indices, set(["attr1", "attr2"]))

    def test_columnNames(self):
        class MyEntity(Entity):
            @Column
            def attr1(self):
                return "str"
            @Column
            def attr2(self):
                return "str"

        o = MyEntity()
        self.assertEqual(o.columnNames, set(["attr1", "attr2"]))

    def test_columnNames_with_pk(self):
        class MyEntity(Entity):
            @PrimaryKey
            def attr1(self):
                return "str"
            @Column
            def attr2(self):
                return "str"
            @Column
            def attr3(self):
                return "str"

        o = MyEntity()
        self.assertEqual(o.columnNames, set(["attr1", "attr2", "attr3"]))

    def test_columnNames_with_pkwcol(self):
        class MyEntity(Entity):
            @PrimaryKey
            @Column
            def attr1(self):
                return "str"
            @Column
            def attr2(self):
                return "str"
            @Column
            def attr3(self):
                return "str"

        o = MyEntity()
        self.assertEqual(o.columnNames, set(["attr1", "attr2", "attr3"]))

    def test_columnNames_with_index(self):
        class MyEntity(Entity):
            @Index
            def attr1(self):
                return "str"
            @Column
            def attr2(self):
                return "str"
            @Column
            def attr3(self):
                return "str"

        o = MyEntity()
        self.assertEqual(o.columnNames, set(["attr1", "attr2", "attr3"]))

    def test_columnNames_with_indexwcol(self):
        class MyEntity(Entity):
            @Index
            @Column
            def attr1(self):
                return "str"
            @Column
            def attr2(self):
                return "str"
            @Column
            def attr3(self):
                return "str"

        o = MyEntity()
        self.assertEqual(o.columnNames, set(["attr1", "attr2", "attr3"]))

    def test_columnType(self):
        class MyEntity(Entity):
            @Column
            def attr1(self):
                return "str"
            @Column
            def attr2(self):
                return "int"

        o = MyEntity()
        self.assertEqual(o.columnType("attr1"), "str", "attr1 type mismatch")
        self.assertEqual(o.columnType("attr2"), "int", "attr2 type mismatch")