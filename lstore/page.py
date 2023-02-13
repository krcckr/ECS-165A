BASE_PAGE_MAXIMUM = 16
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3



class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.index = 0
        pass

    def has_capacity(self):
        return self.num_records * 8 < len(self.data)
        

    def write(self, value):
        bytes = (value).to_bytes(8, 'big')
        for i in range(8):
            self.data[self.index] = bytes[i]
            self.index += 1
        self.num_records += 1
        pass

class Page_range:

    def __init__(self, num_columns):
        self.num_base_record = 0
        self.last_base_page_index = 0
        self.page_range = [None] * (num_columns + 4)
        self.page_range[INDIRECTION_COLUMN] = [Page(),Page()]
        self.page_range[RID_COLUMN] = [Page(),Page()]
        self.page_range[TIMESTAMP_COLUMN] = [Page(),Page()]
        self.page_range[SCHEMA_ENCODING_COLUMN] = [Page(),Page()]
        
        for i in range(num_columns):
            self.page_range[i+4] = [Page(),Page()]
        pass
    
    def has_capacity(self):
        return self.num_base_record // 512 < BASE_PAGE_MAXIMUM
    
    def locate_page(self, column_index, page_type: str) -> Page():
        if(page_type == 'base'):
            return self.page_range[column_index][self.last_base_page_index]
        
        else:
            return self.page_range[column_index][-1]
    
    def write_to_page(self, column_index, value: int, page_type: str): 
        page_to_write = self.locate_page(column_index, page_type) #locate the page to write

        if(page_to_write.has_capacity()):
            page_to_write.write(value)
        else:
            new_page = Page()
            new_page.write(value)
            if(page_type == 'base'):
                index = self.last_base_page_index + 1
                self.page_range[column_index].insert(index, new_page)
            else:
                self.page_range[column_index].append(new_page)
        pass

        

