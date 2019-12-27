import os
import struct


class Page:
    page_size = 512
    datePattern = "yyyy-MM-dd_HH:mm:ss"

    def __init__(self):
        pass

    def get_root_node(self, table_file_path):
        '''
        Read the root page in the file, if there is no root node, create a new node
        '''
        if os.stat(table_file_path).st_size == 0:
            table_root_node = [1, 0, 0]
            with open(table_file_path, "wb") as fh:
                root_node_size = len(table_root_node)
                root_node = struct.pack('i' * root_node_size, *table_root_node)
                fh.write(root_node)
        else:
            '''
            Read the root node cells
            '''
            table_root_node = []

            with open(table_file_path, 'rb') as f:
                f.seek(0, 0)
                i = 0
                node_value = f.read(4)
                while node_value != b'\x00\x00\x00\x00' and i < self.page_size:
                    table_root_node.append(struct.unpack('i', node_value)[0])
                    node_value = f.read(4)
                    i += 4
        return table_root_node

    def check_page_size(self, table_file_path, page_number):

        with open(table_file_path, 'rb') as f:
            f.seek((page_number * self.page_size), 0)
            page_size = 0
            while page_size < 512:
                bytes1 = f.read(1)
                if bytes1 == b'':
                    break
                page_size += 1
        return page_size

    def write_to_page(self, table_file_path, page_number, start_byte, record_values, fstring, record_payload=0):

        with open(table_file_path, "r+b") as fh:
            page_offset = start_byte
            #print("printing to the page at offset",page_offset)
            fh.seek(page_offset, 0)
            record = struct.pack(fstring, *record_values)
            fh.write(record)
            return True,fh.tell()
        return False
    
    def write_to_del_page(self, table_file_path, page_number, start_byte, record_values, fstring, record_payload=0):

        with open(table_file_path, "r+b") as fh:
            page_offset = start_byte
            fh.seek(page_offset, 0)
            record = struct.pack(fstring, *record_values)
            fh.write(record)
            return True,fh.tell()
        return False
    
    def page_clean_bytes(self, table_file_path, page_no):
        page_offset = page_no * self.page_size
        with open(table_file_path, "r+b") as fh:
            fh.seek(page_offset, 0)
            for i in range(0, self.page_size):
                fh.write(b'0')
        return True
    
    def read_page(self, table_file_path, column_dtype, page_number, record_fstring, no_of_records):
        '''
        Read though the page and get all records, since the text datatype is encoded, reading the datatype carefully since
        the encoded string will be stored in bytes of mutiple of 4
        '''
        fstring_value = {"x": 0, "h": 2, "i": 4, "q": 8, "f": 4, "d": 8, "Q": 8, "B": 1, "b": 1, "H": 2, "s": 1,"I":4}
        with open(table_file_path, 'rb') as fh:
            page_offset = page_number * self.page_size
            page_end = page_offset + self.page_size
            page_records = []
            for i in range(0, no_of_records):
                record = []
                for f_str in record_fstring:
                    fh.seek(page_offset, 0)
                    read_bytes = fstring_value[f_str]
                    if f_str != "s":
                        rec_value = fh.read(read_bytes)
                        record.append(struct.unpack(f_str, rec_value)[0])
                    else:
                        counter = 0
                        read_bytes = fstring_value[f_str]
                        text_val = ''
                        text_pg_offset = page_offset
                        while True:
                            if fh.read(2) != b'>x':
                                fh.seek(text_pg_offset, 0)
                                text_val += (struct.unpack(f_str, fh.read(read_bytes))[0]).decode("utf-8")
                                text_pg_offset += 1
                                counter += 1
                            else:
                                counter += 2
                                break
                        if counter % 4 != 0:
                            read_bytes = counter + (4 - (counter % 4))
                        else:
                            read_bytes = counter
                        record.append(text_val)
                    page_offset += read_bytes
                page_records.append(record)

        if len(record) < 1:
            print("Error while reading the page")
            return False, page_records
        else:
            #print("Page read successful")
            return True, page_records

    def update_root_node(self, table_file_path, updated_root,root_offset):
        with open(table_file_path, "r+b") as fh:
            fh.seek(root_offset, 0)
            root_node_size = len(updated_root)
            root_node = struct.pack('i' * root_node_size, *updated_root)
            fh.write(root_node)
