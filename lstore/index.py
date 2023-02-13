"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [OOBTree()] * table.num_columns
        # Only need to implement the first index for milestone 1

    def add(self, column, key, rid):
        self.indices[column].insert(key=key, value=rid)
    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        return self.indices[column].get(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return list(self.indices[column].values(min=begin, max=end))

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
