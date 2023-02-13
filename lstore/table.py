from lstore.index import Index
from lstore.page import Page
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
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.page_range = []
        self.page = Page()
        self.table = []

        #Allocate all the space for records, INDIRECTION_COLUMN, RID_COLUMN,TIMESTAMP_COLUMN, and SCHEMA_ENCODING_COLUMN
        for i in range(num_columns + 4):
            emptylist = []
            self.table.append(emptylist)
            self.page_range.append(emptylist)
        self.rid = 0

        pass

    def __merge(self):
        print("merge is happening")
        pass
        
    def insert(self, *columns):
        schema_encoding = '0' * self.num_columns
        key = columns[self.key]
        

        #if the key exsit return false
        if(key in self.table[self.key + 4]):
            return False
        
        else:
            self.rid += 1 #assign next rid to base record
            record_index = 4 #the first column that store the record is column 4
            #create a base record:
            record = Record(self.rid, key, columns)

            #insert the schema_encoding column:
            self.table[SCHEMA_ENCODING_COLUMN] = [schema_encoding]

            #insert the rid column:
            self.table[RID_COLUMN].append(record.rid)

            #update the indirection column:
            self.table[INDIRECTION_COLUMN].append(record.rid)
            
            #insert the record seperately to implement a columnar structrue 
            for value in columns:
                self.table[record_index].append(value)
                
                #write the record to page and add the page to page range
                if(self.page.has_capacity()):
                    self.page.write(value)
                    self.page_range[record_index] = [self.page]
                else:
                    self.page = Page() #create an empty page to write
                    self.page.write(value)
                    self.page_range[record_index] = [self.page]

                record_index += 1
            
            return True
                
                
            

