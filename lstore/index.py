"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.

        # replace an array with a binarry-tree-like data structure

        self.indices = table

        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, value):
        return self.indices.get(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # TODO: ask TA if begin and end included and if begin could be bigger than end

        values = list[self.indices.values(begin, end)]

        return values


    def insert_item(self, key, RID):
        rids = self.indices.get(key)

        if rids:
            self.indices.update({key: rids.append(RID)})

        else:   
            self.indices.update({key: [RID]})

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        return(self.indices.update(column_number, column_number))
        

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        return(self.indices.delete(column_number))
