# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 20:55:06 2019

@author: kavin
"""

import math
import os
import datetime
import time
from tabulate import tabulate
from Page import Page


class Table(Page):
    page_size = 512
    datePattern = "yyyy-MM-dd_HH:mm:ss"

    def __init__(self, table_name):
        self.table_name = table_name
        self.data_dir = os.path.join(os.getcwd(), 'data')
        self.table_dir = self.data_dir + "/" + self.table_name
        self.table_file_path = self.table_dir + "/" + self.table_name + ".tbl"
        self.dtype_bytes = {"null": 0, "tinyint": 2, "smallint": 2, "int": 4, "bigint": 8, "long": 8,
                            "float": 4, "double": 8, "year": 4, "time": 4, "datetime": 4, "date": 4}
        self.struct_format_string = {"null": "x", "tinyint": 'b', "smallint": 'h', "int": 'i',
                                     "bigint": "q", "long": "q", "float": "f", "double": "d", "year": "i", "time": "I",
                                     "datetime": "I", "date": "I"}
        self.accepted_operator = ["=", ">", ">=", "<", "<=", "<>"]

    # Create a table if the table already didn't exists
    def create_table(self, table_name):
        self.__init__(table_name)
        try:
            if not os.path.isdir(self.data_dir):
                os.makedirs(self.table_dir)
            else:
                os.mkdir(self.table_dir)
            with open(self.table_file_path, 'wb') as f:
                print(self.table_name + " table is created")
                return self.table_file_path
        except FileExistsError:
            print("Table already exists..You cannot create the same table again!")

    # Check if the tale exist in the database already by checking the catalog
    def check_if_table_exists(self, table_path):
        return os.path.exists(table_path)
    
    #converting text to format string or if any data structure to respective format string
    def values_to_fstring(self, col_dtype, values):
        fstring = " "
        for dt in range(0, len(col_dtype)):
            if col_dtype[dt] in self.struct_format_string.keys():
                fstring += self.struct_format_string[col_dtype[dt]]
            elif col_dtype[dt] == "text":
                fstring += str(len(values[dt])) + "s"
            fstring += " "
        fstring = fstring.strip()
        return fstring
    
    def schema_to_fstring(self, col_dtype):
        fstring = " "
        for dt in range(0, len(col_dtype)):
            if col_dtype[dt] in self.struct_format_string.keys():
                fstring += self.struct_format_string[col_dtype[dt]]
            elif col_dtype[dt] == "text":
                fstring += "s"
        fstring = fstring.strip()
        return fstring
    
    #encoding the text data to utf-8
    def string_encoding(self, record):
        r_values = []
        for r in record:
            if type(r) is str:
                r_values.append(r.encode('utf-8'))
            else:
                r_values.append(r)
        return r_values

    #converting time to milliseconds
    def time_to_milli(self, t):
        hours, minutes, seconds = (["0", "0"] + t.split(":"))[-3:]
        hours = int(hours)
        minutes = int(minutes)
        seconds = float(seconds)
        miliseconds = int(3600000 * hours + 60000 * minutes + 1000 * seconds)
        return miliseconds
    
    #converting milliseconds back to time
    def milli_to_time(self, m):
        seconds = (m / 1000) % 60
        seconds = int(seconds)
        minutes = (m / (1000 * 60)) % 60
        minutes = int(minutes)
        hours = (m / (1000 * 60 * 60)) % 24
        hours = int(hours)
        ti = str(hours) + ":" + str(minutes) + ":" + str(seconds)
        return ti
    #converting time to long interger format
    def date_time_epoch_to_bytes(self, date_time, conv):
        if conv == "datetime":
            pattern = '%m.%d.%Y %H:%M:%S'
        elif conv == "date":
            pattern = '%m.%d.%Y'
        epoch = int(time.mktime(time.strptime(date_time, pattern)))
        return epoch

    def bytes_to_date_time(self, epoch, conv):
        ts = datetime.datetime.fromtimestamp(epoch)
        if conv == "datetime":
            ret_val = ts.strftime('%m.%d.%Y %H:%M:%S')
        elif conv == "date":
            ret_val = ts.strftime('%m.%d.%Y')
        return ret_val

    def string_from_date_time(self, col_dtype, values):
        for dt in range(0, len(col_dtype)):
            if col_dtype[dt] == "year":
                values[dt] = str(values[dt])
            elif col_dtype[dt] == "time":
                values[dt] = self.milli_to_time(values[dt])
            elif col_dtype[dt] == "date":
                values[dt] = self.bytes_to_date_time(values[dt], "date")
            elif col_dtype[dt] == "datetime":
                values[dt] = self.bytes_to_date_time(values[dt], "datetime")
        return values

    def date_time_conv(self, col_dtype, values):
        for dt in range(0, len(col_dtype)):
            if col_dtype[dt] == "year":
                values[dt] = int(values[dt])
            elif col_dtype[dt] == "time":
                values[dt] = self.time_to_milli(values[dt])
            elif col_dtype[dt] == "date":
                values[dt] = self.date_time_epoch_to_bytes(values[dt], "date")
            elif col_dtype[dt] == "datetime":
                values[dt] = self.date_time_epoch_to_bytes(values[dt], "datetime")
        return values

    def explicit_type_conv(self, col_dtype, values):
        if len(col_dtype) == len(values):
            for dt in range(0, len(col_dtype)):
                if col_dtype[dt] == "float":
                    values[dt] == float(values[dt])
                elif col_dtype[dt] == "int":
                    values[dt] == int(values[dt])
        else:
            print("Error while type conversion")
        return values

    def insert_into_table(self, table_name, values):
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please create a table first")
            return False
        # print("Table is existing")
        root_node = self.get_root_node(self.table_file_path)
        # print("returned root node is", root_node)
        
        #getting column datatype , constraints and names from schema
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        dtype_wo_pri = col_dtype[1:]
        for val in range(0, len(values)):
            if dtype_wo_pri[val] == "text":
                values[val] = (values[val] + ">x")
    
        values = self.date_time_conv(dtype_wo_pri, values)
        record_payload = self.calculate_payload_size([0] + values)
        # check the number of pages in the table
        if record_payload > 512:
            print("Record size is greater than 512 bytes..Cannot accommodate the record in the table")
        # Checking if the left-leaf node exists
        page_number = root_node[-3]
        page_total_record = root_node[-2]
        page_last_rowid = root_node[-1]
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        insert_success = False
        
        #check if the root page is empty, start adding the records at page-1
        if page_last_rowid == 0 and record_payload < 512:
            row_id = 1
            record = self.string_encoding([row_id] + values)
            page_offset = page_number * self.page_size
            fstring = self.values_to_fstring(col_dtype, record)
            record = self.explicit_type_conv(col_dtype, record)
            #print("Creating1 new record", page_number, page_offset, record, fstring)
            #Writing to page 1
            insert_success, temp_var = self.write_to_page(self.table_file_path, page_number, page_offset, record,
                                                          fstring,
                                                          record_payload)
            if insert_success:
                root_node_len = len(root_node)
                if root_node_len == 3:
                    root_offset = 0
                self.update_root_node(self.table_file_path, [page_number, page_total_record + 1, row_id], root_offset)
        else:
            '''
            After checking the root node, check the last leaf page size and compute the filled size, find the starting 
            position where the record can be added
            '''
            page_filled_size = self.check_page_size(self.table_file_path, page_number)
            # print("Filled Page size", page_filled_size)
            page_size_availability = self.page_size - page_filled_size
            # print("Page size availability", page_size_availability)
            if record_payload <= page_size_availability:
                page_offset = (page_filled_size) + page_number * self.page_size
                record = self.string_encoding([page_last_rowid + 1] + values)
                fstring = self.values_to_fstring(col_dtype, record)
                #print("Creating2 new record", page_number, page_offset, record, fstring)
                record = self.explicit_type_conv(col_dtype, record)
                #Add record to the available position of the page
                insert_success, temp_var = self.write_to_page(self.table_file_path, page_number, page_offset, record,
                                                              fstring,
                                                              record_payload)
                if insert_success:
                    root_node_len = len(root_node)
                    if root_node_len == 3:
                        root_offset = 0
                    else:
                        root_offset = ((root_node_len // 3) - 1) * 12
                    #If the records have been added successfully,update the root node by updating the rowid and total number of records
                    self.update_root_node(self.table_file_path,
                                          [page_number, page_total_record + 1, page_last_rowid + 1], root_offset)
            else:
                '''
                Read the root node and check the availability of last page and if the page is filled, create a new page of size of 512bytes
                After addinng the records to the page , add a new cell to root node updating the details of newly added page
                '''
                # print("Creating new page")
                new_page_number = page_number + 1
                new_page_rowid = page_last_rowid + 1
                page_total_record = 1
                page_offset = new_page_number * self.page_size
                record = self.string_encoding([new_page_rowid] + values)
                fstring = self.values_to_fstring(col_dtype, record)
                #print("Creating3 new record", page_number, page_offset, record, fstring)
                record = self.explicit_type_conv(col_dtype, record)
                insert_success, temp_var = self.write_to_page(self.table_file_path, new_page_number, page_offset,
                                                              record,
                                                              fstring, record_payload)
                if insert_success:
                    root_offset = ((len(root_node)) // 3) * 12
                    self.update_root_node(self.table_file_path,
                                          [new_page_number, page_total_record, new_page_rowid], root_offset)
        if insert_success:
            print("Record has been successfully added")
            return True
        else:
            print("Error occurred while adding new record")
            return False

    def traverse_tree(self, table_name):
        '''
        Using the root page, get the details of the available record pages   
        1) Page number
        2) Number of cells in the pages
        3) Last row id in the page 
        Read the each page records recursively, get all the records in all pages
        '''   
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
            return False
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        s_fstring = self.schema_to_fstring(col_dtype)
        root_node = self.get_root_node(self.table_file_path)
        page_records = []
        for page_no in range(0, len(root_node), 3):
            #print("for the page number", root_node[page_no], col_dtype, s_fstring)
            ret_val, record_val = self.read_page(self.table_file_path, col_dtype, int(root_node[page_no]), s_fstring,
                                                 root_node[page_no + 1])
            if ret_val:
                page_records += record_val
            else:
                print("Error while traversing through Tree")
                break
        return page_records

    def delete_record(self, table_name, column, operator, value, is_not=False):
        '''
        Read through the pages in the table, delete the records that matches the conditions sent from the command prompt
        '''
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
            return False
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        if column not in column_names:
            return False
        if operator not in self.accepted_operator:
            return False
        column_index = column_names.index(column)
        s_fstring = self.schema_to_fstring(col_dtype)
        root_node = self.get_root_node(self.table_file_path)
        value = self.date_time_conv([col_dtype[column_index]], [value])[0]
        page_records = []
        total_deleted_records = []
        insert_success = False
        for page_no in range(0, len(root_node), 3):
            page_number = root_node[page_no]
            page_total_recs = root_node[page_no + 1]
            page_last_rid = root_node[page_no + 2]
            ret_val, record_val = self.read_page(self.table_file_path, col_dtype, page_number, s_fstring,
                                                 page_total_recs)
            if ret_val:
                
                #Get matched records that need to deleted
                deleted_records, new_page_records = self.column_condition_check(record_val, operator, value,
                                                                                column_index, is_not)
            else:
                print("Error while traversing through Tree")
                break

            for dr in deleted_records:
                total_deleted_records.append(dr)

            if len(total_deleted_records) > 0:
                #Since the page size is of 512 bytes, page can be cleaned and unaffected records can be added back  efficiently
                # to free up the memory space taken by the deleted records
                
                self.page_clean_bytes(self.table_file_path, page_number)
                page_offset = page_number * self.page_size
                for record in new_page_records:
                    for val in range(0, len(record)):
                        if col_dtype[val] == "text":
                            record[val] = (record[val] + ">x")
                    record = self.string_encoding(record)
                    fstring = self.values_to_fstring(col_dtype, record)
                    record_payload = self.calculate_payload_size(record)
                    #print("wrting to page", page_number, page_offset, record, fstring, record_payload)
                    record = self.explicit_type_conv(col_dtype, record)
                    insert_success, page_offset = self.write_to_del_page(self.table_file_path, page_number, page_offset,
                                                                         record, fstring,
                                                                         record_payload)
                if insert_success:
                    root_node_len = len(root_node)
                    if root_node_len == 3:
                        root_offset = 0
                    else:
                        root_offset = (page_no // 3) * 12
                    page_total_recs = len(new_page_records)
                    #print("updating the root node", page_number, page_total_recs, page_last_rid, root_offset)
                    self.update_root_node(self.table_file_path, [page_number, page_total_recs, page_last_rid],
                                          root_offset)
                else:
                    print("Error while writing into page")
            else:
                continue
        return True

    def calculate_payload_size(self, values):
        '''
        Calculate the total bytes that would be taken by the records
        '''
        #Hard-coding the Meta data details
        self.table_desc = {"row_id": {"datatype": "int", "constraints": "pri:not null"},
                           "person_id": {"datatype": "int", "constraints": "not null"},
                           "name": {"datatype": "text", "constraints": "not null"},
                           "dob": {"datatype": "date", "constraints": "not null"},
                           "email": {"datatype": "text", "constraints": ""},
                           "dept_no": {"datatype": "int", "constraints": "not null"}}
        # table_desc = self.table_dtype_constraint()
        dtype_bytes = []
        for index, (col, col_cons) in enumerate(self.table_desc.items()):
            if col_cons["datatype"] in self.dtype_bytes:
                dtype_bytes.append(self.dtype_bytes[col_cons["datatype"]])

            if col_cons["datatype"] == "text":
                dtype_bytes.append(len(values[index]))
        payload_size = sum(dtype_bytes)
        return payload_size

    # get the datatype,constraints from the meta-data
    def scheme_dtype_constraint(self):
        self.table_desc = {"row_id": {"datatype": "int", "constraints": "pri:not null"},
                           "person_id": {"datatype": "int", "constraints": "not null"},
                           "name": {"datatype": "text", "constraints": "not null"},
                           "dob": {"datatype": "date", "constraints": "not null"},
                           "email": {"datatype": "text", "constraints": ""},
                           "dept_no": {"datatype": "int", "constraints": "not null"}}

        self.table_constraints = []
        self.table_dtypes = []
        self.column_names = []
        for (col, col_cons) in self.table_desc.items():
            self.table_dtypes.append(col_cons["datatype"])
            self.table_constraints.append(col_cons["constraints"])
            self.column_names.append(col)
        return self.table_dtypes, self.table_constraints, self.column_names

    def column_condition_check(self, record_val, cond_operator, value, column_index, is_not=False):
        '''
        Select the records that meet the given conditions\
        '''
        impacted_records = []
        unimpacted_records = []
        for rec in range(0, len(record_val)):
            if cond_operator == "=":
                if is_not:
                    if not record_val[rec][column_index] == value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] == value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == ">":
                if is_not:
                    if not record_val[rec][column_index] > value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] > value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == ">=":
                if is_not:
                    if not record_val[rec][column_index] >= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] >= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == "<":
                if is_not:
                    if not record_val[rec][column_index] < value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] < value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == "<=":
                if is_not:
                    if not record_val[rec][column_index] <= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] <= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == "<>":
                if is_not:
                    if not record_val[rec][column_index] != value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] > value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])

        return impacted_records, unimpacted_records

    def update_matched_records(self, updated_records, set_column, set_value, set_column_index):
        for rec in range(0, len(updated_records)):
            updated_records[rec][set_column_index] = set_value
        return updated_records

    def update_record(self, table_name, set_column, set_value, cond_column, cond_operator, cond_value, is_not=False):
        '''
        Read the records from the pages and update the records which match the given condition and write back in the page
        '''
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
            return False
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        if set_column not in column_names and cond_column not in column_names:
            return False
        if cond_operator not in self.accepted_operator:
            return False
        set_column_index = column_names.index(set_column)
        cond_column_index = column_names.index(cond_column)
        s_fstring = self.schema_to_fstring(col_dtype)
        root_node = self.get_root_node(self.table_file_path)
        cond_value = self.date_time_conv([col_dtype[cond_column_index]], [cond_value])[0]
        set_value = self.date_time_conv([col_dtype[set_column_index]], [set_value])[0]
        deleted_records = []
        root_last_rid = root_node[-1]
        move_records = []
        for page_no in range(0, len(root_node), 3):
            page_number = root_node[page_no]
            page_total_recs = root_node[page_no + 1]
            page_last_rid = root_node[page_no + 2]
            ret_val, record_val = self.read_page(self.table_file_path, col_dtype, int(page_number), s_fstring,
                                                 page_total_recs)

            if ret_val:
                #print("column condition checking", record_val, cond_operator, cond_value, cond_column_index, is_not)
                updated_records, n_page_records = self.column_condition_check(record_val, cond_operator, cond_value,
                                                                              cond_column_index, is_not)
            else:
                print("Error while traversing through Tree")
                break
            #print("updated records are ", updated_records)
            #print("Old records are ", n_page_records)
            if len(updated_records) > 0:
                # print("checking for the udpated recorsd", updated_records, set_column, set_value, set_column_index)
                updated_records = self.update_matched_records(updated_records, set_column, set_value, set_column_index)
                records_size = 0

                for rec in n_page_records:
                    records_size += self.calculate_payload_size(rec)
                for rec in updated_records:
                    rec_size = self.calculate_payload_size(rec)
                    if (records_size + rec_size) < self.page_size:
                        n_page_records.append(rec)
                        records_size = records_size + rec_size
                    else:
                        move_records.append(rec)

                page_offset = page_number * self.page_size
                for record in n_page_records:
                    for val in range(0, len(record)):
                        if col_dtype[val] == "text":
                            record[val] = (record[val] + ">x")
                    record = self.string_encoding(record)
                    fstring = self.values_to_fstring(col_dtype, record)
                    record_payload = self.calculate_payload_size(record)
                    #print("wrting to page", page_number, page_offset, record, fstring, record_payload)
                    insert_success, page_offset = self.write_to_page(self.table_file_path, page_number, page_offset,
                                                                     record, fstring,
                                                                     record_payload)
                if insert_success:
                    root_node_len = len(root_node)
                    if root_node_len == 3:
                        root_offset = 0
                    else:
                        root_offset = (page_no // 3) * 12
                    page_total_recs = len(n_page_records)
                    # print("updating the root node", page_number, page_total_recs, page_last_rid, root_offset)
                    self.update_root_node(self.table_file_path, [page_number, page_total_recs, page_last_rid],
                                          root_offset)
                    # print("updated root node is", self.get_root_node(self.table_file_path))
                else:
                    print("Error while writing into page")
            else:
                # print("no change in this page")
                continue
        for record in range(len(move_records)):
            record = self.string_from_date_time(col_dtype, record)
            self.insert_into_table(self.table_name, record[1:])
        return True

    def select_from_table(self, table_name, select_columns, cond_column=None, cond_operator=None, cond_value=None,
                          is_not=None):
        '''
        Read through the pages in the file and give all the records that match the condition
        '''
        self.__init__(table_name)
        all_records = self.traverse_tree(table_name)
        temp_record = []
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        for rec in range(len(all_records)):
            temp_record.append(self.string_from_date_time(col_dtype, all_records[rec]))
        all_records.clear()
        all_records = temp_record.copy()
        if cond_column is None or cond_operator is None or cond_value is None:
            selected_records = all_records.copy()
        else:
            if cond_column not in column_names:
                return False
            cond_column_index = column_names.index(cond_column)
            #print("condition check", all_records, cond_operator, cond_value, cond_column_index, is_not)
            selected_records, n_page_records = self.column_condition_check(all_records, cond_operator, cond_value,
                                                                           cond_column_index, is_not)
        select_column_index = []
        if select_columns == ['*']:
            matched_records = selected_records.copy()
            selected_col_names = column_names.copy()
        else:
            for col in select_columns:
                if col in column_names:
                    select_column_index.append(column_names.index(col))
            selected_col_names = []
            for col in select_column_index:
                selected_col_names.append(column_names[col])
            matched_records = []

            for rec in range(len(selected_records)):
                rec_matched_index = []
                for ind in select_column_index:
                    rec_matched_index.append(selected_records[rec][ind])
                matched_records.append(rec_matched_index)
        return matched_records, selected_col_names


Table = Table("person_details")

print("Table Creation\n")
Table.create_table("person_details")

print("Table Insert\n")
Table.insert_into_table("person_details", [100, "Dotty", "07.01.2019", "deastup0@google.nl", 62])
Table.insert_into_table("person_details", [101, "Aksel", "03.01.2019", "agoldson1@tiny.cc", 20])
Table.insert_into_table("person_details", [102, "Trixie", "02.02.2019", "tdaniellot6@flickr.com", 17])
Table.insert_into_table("person_details", [103, "Reggy", "01.02.2019", "rlapid9@mtv.com", 53])
records, columns = Table.select_from_table("person_details",['*'])

print("Table Record Deletion\n")
column = "name"
operator = "="
value = "Trixie"
is_not = False
Table.delete_record("person_details", column, operator, value, is_not)
records, columns = Table.select_from_table("person_details",['*'])
print(tabulate(records, headers=columns))

print("Table Record Updation\n")
set_column = "person_id"
set_value = 125
cond_column = "name"
cond_operator = "="
cond_value = "Reggy"
is_not = False
Table.update_record("person_details", set_column, set_value, cond_column, cond_operator, cond_value, is_not)
records, columns = Table.select_from_table("person_details",['*'])
print(tabulate(records, headers=columns))

print("Table Record Selection\n")
select_columns = ["name","dob"]
cond_column = "dept_no"
cond_operator = ">="
cond_value = 20
is_not = False
records, columns = Table.select_from_table("person_details", select_columns, cond_column, cond_operator, cond_value,
                                           is_not)
print(tabulate(records, headers=columns))
