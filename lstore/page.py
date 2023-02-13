
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.index = 0

    def has_capacity(self):
        return self.num_records * 8 < len(self.data)
        

    def write(self, value):
        bytes = value.to_bytes(8, 'big')
        for i in range(8):
            self.data[self.index] = bytes[i]
            self.index += 1
        self.num_records += 1
        pass

