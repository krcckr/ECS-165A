from lstore.index import Index
from lstore.page import Page, Page_range
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.keybuffer = [key]
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.page_ranges = [Page_range(num_columns)]
        self.rid = 0


    def __merge(self):
        print("merge is happening")
        pass
        
    def insert(self, *columns):
        schema_encoding = int('0' * len(columns))
        key = columns[self.key]
        
        #if the key exsit return false
        if(key in self.keybuffer):
            return False
        
        else:
            self.keybuffer.append(key)
            self.rid += 1 #assign next rid to base record
            
            #insert the record seperately to implement a columnar structrue 
            page_range = self.page_ranges[-1]
            if(page_range.has_capacity()):
                
                page_range.write_to_page(INDIRECTION_COLUMN, self.rid, 'base')
                page_range.write_to_page(RID_COLUMN, self.rid, 'base')
                page_range.write_to_page(TIMESTAMP_COLUMN, 0, 'base')
                page_range.write_to_page(SCHEMA_ENCODING_COLUMN, schema_encoding, 'base')

                for value in columns:
                    value_index = 4 #value column starts from the 4th column
                    page_range.write_to_page(value_index, value, 'base')
                    value_index += 1
                
                page_range.num_base_record += 1
            
            else:
                new_page_range = Page_range(self.num_columns)
                new_page_range.write_to_page(INDIRECTION_COLUMN, self.rid, 'base')
                new_page_range.write_to_page(RID_COLUMN, self.rid, 'base')
                new_page_range.write_to_page(TIMESTAMP_COLUMN, 0, 'base')
                new_page_range.write_to_page(SCHEMA_ENCODING_COLUMN, schema_encoding, 'base')

                for value in columns:
                    value_index = 4 #value column starts from the 4th column
                    page_range.write_to_page(value_index, value, 'base')
                    value_index += 1
                
                new_page_range.num_base_record += 1
                self.page_ranges.append(new_page_range)

            return True
                
                
            

