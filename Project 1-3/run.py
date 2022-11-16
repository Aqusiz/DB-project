# python 3.9.6
# berkeley-db 5.3.28
# pip berkeleydb 18.1.5
from lark import Lark, Transformer, exceptions
from berkeleydb import db
import pickle
import os
import datetime

MY_PROMPT = "DB_2017-16140> "
catalogDB = db.DB()

class TreeParser():
    def parse(self, root):
        if root.data == "table_element_list":
            return self.parse_table_element_list(root)
    # Build table schema dictinary and return it
    def parse_table_element_list(self, root):
        table_dict = {"columns": {}}
        # parse tree into column_definition, primary_key_constraint, foreign_key_constraint
        col_defs = root.find_data("column_definition")
        pk_constraint = list(root.find_data("primary_key_constraint"))
        ref_constraints = root.find_data("referential_constraint")
        # parse column definitions
        for col_def in col_defs:
            col_name, col_type, col_nullable = self.parse_column_definition(col_def)
            if col_name in table_dict["columns"]:
                raise Exception("DuplicateColumnDefError")
            table_dict["columns"][col_name] = {
                "type": col_type, 
                "nullable": col_nullable,
                "primary_key": False,
                "references": None
                }
        # parse primary key constraint
        if len(pk_constraint) > 1:
            raise Exception("DuplicatePrimaryKeyDefError")
        pk_list = self.parse_primary_key_constraint(pk_constraint[0])
        for pk in pk_list:
            if pk not in table_dict["columns"]:
                raise Exception("NonExistingColumnDefError("+pk+")", pk)
            table_dict["columns"][pk]["nullable"] = False
            table_dict["columns"][pk]["primary_key"] = True
        # parse foreign key constraint
        for ref_con in ref_constraints:
            refer_info = self.parse_referential_constraint(ref_con)
            for col_name, ref_name in zip(refer_info["col_names"], refer_info["ref_names"]):
                if col_name not in table_dict["columns"]:
                    raise Exception("NonExistingColumnDefError("+col_name+")", col_name)
                if table_dict["columns"][col_name]["references"] is not None:
                    raise Exception("DuplicateForeignKeyDefError("+col_name+")", col_name)
                table_dict["columns"][col_name]["references"] = refer_info["ref_table_name"]+"."+ref_name

        table_dict["referenced_by"] = []
        table_dict["pk_list"] = pk_list
        return table_dict

    def parse_column_definition(self, root):
        col_name = root.children[0].children[0].value.lower()
        for token in root.children[1].children:
            if token.type == "INT" and int(token.value) < 1:
                raise Exception("CharLengthError")
        col_type = "".join(x.value.lower() for x in root.children[1].children)
        col_nullable = (root.children[2] is None)
        
        return col_name, col_type, col_nullable

    def parse_table_constraint_definition(self, root):
        if root.children[0].data == "primary_key_constraint":
            constraint_name = "PRIMARY KEY"
            constraint_data = self.parse_primary_key_constraint(root.children[0])
        else:
            constraint_name = "FOREIGN KEY"
            constraint_data = self.parse_referential_constraint(root.children[0])
        
        return constraint_name, constraint_data

    def parse_primary_key_constraint(self, root):
        pk_list = []
        column_list = root.find_data("column_name")
        for column in column_list:
            column_name = column.children[0].value.lower()
            if column_name in pk_list:
                raise Exception("DuplicatePrimaryKeyDefError")
            else:
                pk_list.append(column_name)
        
        return pk_list

    def parse_referential_constraint(self, root):
        col_name_list = root.children[2].find_data("column_name")
        col_names = [x.children[0].value.lower() for x in col_name_list]

        ref_table_name = root.children[4].children[0].value.lower()

        ref_name_list = root.children[5].find_data("column_name")
        ref_names = [x.children[0].value.lower() for x in ref_name_list]

        refer_info = {
            "col_names": col_names,
            "ref_table_name": ref_table_name,
            "ref_names": ref_names
        }
        return refer_info

class T(Transformer):
    # items[0] == Token "CREATE"
    # items[1] == Token "TABLE"
    # items[2] == Tree "table_name"
    # items[3] == Tree "table_element_list"
    def create_table_query(self, items):
        table_name = items[2].children[0].value.lower()
        tables = pickle.loads(catalogDB.get(b"tables"))
        # check if table already exists
        if table_name in tables:
            raise Exception("TableExistenceError")
        tables.append(table_name)
        # parse query and create table schema dictionary
        table_dict = TreeParser().parse(items[3])
        # check referential integrity
        referenced_table_dict = {}
        for col_name, col_info in table_dict["columns"].items():
            if col_info["references"] is not None:
                ref_table_name, ref_col_name = col_info["references"].split(".")
                if ref_table_name not in tables or ref_table_name == table_name:
                    raise Exception("ReferenceTableExistenceError")
                if ref_table_name not in referenced_table_dict:
                    referenced_table_dict[ref_table_name] = pickle.loads(catalogDB.get(ref_table_name.encode()))
                    referenced_table_dict[ref_table_name]["referenced_number"] = 1
                else:
                    referenced_table_dict[ref_table_name]["referenced_number"] += 1
                if ref_col_name not in referenced_table_dict[ref_table_name]["columns"]:
                    raise Exception("ReferenceColumnExistenceError")
                if ref_col_name not in referenced_table_dict[ref_table_name]["pk_list"]:
                    raise Exception("ReferenceNonPrimaryKeyError")
                if col_info["type"] != referenced_table_dict[ref_table_name]["columns"][ref_col_name]["type"]:
                    raise Exception("ReferenceTypeError")
        # update catalogDB
        for ref_name, ref_table_info in referenced_table_dict.items():
            if ref_table_info["referenced_number"] != len(ref_table_info["pk_list"]):
                raise Exception("ReferenceTypeError")
            del(ref_table_info["referenced_number"])
            ref_table_info["referenced_by"].append(table_name)
            catalogDB.put(ref_name.encode(), pickle.dumps(ref_table_info))

        catalogDB.put(b"tables", pickle.dumps(tables))
        catalogDB.put(table_name.encode(), pickle.dumps(table_dict))
        targetDB = db.DB()
        targetDB.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

        targetDB.close()
        print(MY_PROMPT + "'"+table_name+"'" + " table is created")
        
    # items[0] == Token "DROP"
    # items[1] == Token "TABLE"
    # items[2] == Tree "table_name"
    def drop_table_query(self, items):
        table_name = items[2].children[0].value
        tables = pickle.loads(catalogDB.get(b"tables"))
        # check if table exists
        if table_name not in tables:
            print(MY_PROMPT + "No such table")
            return
        target = pickle.loads(catalogDB.get(table_name.encode()))
        # check if table is referenced by other tables
        if len(target["referenced_by"]) != 0:
            print(MY_PROMPT + "Drop table has failed: '" + table_name +"' is referenced by other table")
            return
        target = pickle.loads(catalogDB.get(table_name.encode()))
        referencing_tables = []
        # check if table is referencing other tables
        for column in target["columns"]:
            if target["columns"][column]["references"] is not None:
                ref_table_name, ref_col_name = target["columns"][column]["references"].split(".")
                if ref_table_name not in referencing_tables:
                    referencing_tables.append(ref_table_name)
        for ref_table_name in referencing_tables:
            ref_table_info = pickle.loads(catalogDB.get(ref_table_name.encode()))
            ref_table_info["referenced_by"].remove(table_name)
            catalogDB.put(ref_table_name.encode(), pickle.dumps(ref_table_info))
        # update catalogDB
        tables.remove(table_name)
        catalogDB.put(b"tables", pickle.dumps(tables))
        catalogDB.delete(table_name.encode())
        os.remove('./DB/' + table_name + '.db')
        print(MY_PROMPT + "'" + table_name + "' table is dropped")

    def desc_query(self, items):
        table_name = items[1].children[0].value
        tables = pickle.loads(catalogDB.get(b"tables"))
        # check if table exists
        if table_name not in tables:
            print(MY_PROMPT + "No such table")
            return
        target = pickle.loads(catalogDB.get(table_name.encode()))
        # print table schema
        print("-------------------------------------------------")
        print("table name [" + table_name + "]")
        print(f"{'column name':21s}{'type':11s}{'null':11s}{'key':10s}")
        for col_name, col_info in target["columns"].items():
            nullable = "Y" if col_info["nullable"] else "N"
            key = "PRI" if col_info["primary_key"] else ""
            if col_info["references"] is not None:
                key += "/FOR" if key != "" else "FOR"
            print(f"{col_name:20s} {col_info['type']:10s} {nullable:10s} {key:10s}")
        print("-------------------------------------------------")

    def show_tables_query(self, items):
        tables = pickle.loads(catalogDB.get(b"tables"))
        print("----------------")
        for table in tables:
            print(table)
        print("----------------")

    # items[0] == Token "SELECT"
    # items[1] == Tree "select_list"
    # items[2] == Tree "table_expression"
    def select_query(self, items):
        db_table_list = pickle.loads(catalogDB.get(b"tables"))
        
        # parse from clause
        table_name_list = []
        table_alias_list = []

        from_clause = items[2].children[0]
        table_reference_list = from_clause.children[1].children
        for reffered_table in table_reference_list:
            table_name = reffered_table.children[0].children[0].value
            if table_name not in db_table_list:
                print(MY_PROMPT + "No such table")
                return
            table_name_list.append(table_name)
            # check table alias
            if reffered_table.children[2] is not None:
                table_alias_list.append(reffered_table.children[2].children[0].value)
            else:
                table_alias_list.append(None)


        # parse select clause / not * case
        column_name_list = []
        column_alias_list = []
        column_table_name_list = []
        column_table_alias_list = []

        select_list = items[1]
        if len(select_list.children) > 0:
            for selected_column in select_list.children:
                table_name = table_name_list[0]
                table_alias = None
                col_name = selected_column.children[1].children[0].value
                col_alias = None
                # parse table name
                if selected_column.children[0] is not None:
                    name = selected_column.children[0].children[0].value
                    if name in table_alias_list:
                        table_alias = name
                        table_name = table_name_list[table_alias_list.index(table_alias)]
                    elif name in table_name_list:
                        table_name = name
                    else:
                        print(MY_PROMPT + "No such table")
                        return
                # parse column alias
                if selected_column.children[3] is not None:
                    col_alias = selected_column.children[3].children[0].value
                column_name_list.append(col_name)
                column_alias_list.append(col_alias)
                column_table_name_list.append(table_name)
                column_table_alias_list.append(table_alias)


        # join tables
        joined_rows = [[]]
        joined_cols = []
        for table in table_name_list:
            # make joined column list
            column_list = pickle.loads(catalogDB.get(table.encode()))["columns"].keys()
            column_list = list(column_list)
            for column in column_list:
                if column in column_alias_list: 
                    col_alias = column_alias_list[column_name_list.index(column)]
                else:
                    col_alias =  None
                joined_cols.append([table, table_alias_list[table_name_list.index(table)], column, col_alias])
            
            # make joined rows
            targetDB = db.DB()
            targetDB.open('./DB/' + table + '.db')
            old_size = len(joined_rows)
            for i in range(old_size):
                old_row = joined_rows.pop(0)
                for row in targetDB.values():
                    new_row = old_row + row.decode().split("*")
                    joined_rows.append(new_row)
            targetDB.close()
        

        # select all columns when select *
        if len(select_list.children) == 0:
            for col in joined_cols:
                column_name_list.append(col[2])
                column_alias_list.append(col[3])
                column_table_name_list.append(col[0])
                column_table_alias_list.append(col[1])


        # check where clause for each row
        if items[2].children[1] is not None:
            where_clause = items[2].children[1]
            bool_expr = where_clause.children[1]
            size = len(joined_rows)
            for i in range(size):
                row = joined_rows.pop(0)
                test = test_bool_expr(joined_cols, row, bool_expr)
                if test == True:
                    joined_rows.append(row)
        
        # project rows by selected columns
        size = len(joined_rows)
        for i in range(size):
            row = joined_rows.pop(0)
            new_row = []
            size2 = len(joined_cols)
            for j in range(size2):
                size3 = len(column_name_list)
                for k in range(size3):
                    if column_name_list[k] == joined_cols[j][2] and column_table_name_list[k] == joined_cols[j][0]:
                        new_row.append(row[j])
            joined_rows.append(new_row)

        # redundant column name processing with table name
        for i in range(len(column_name_list)):
            marked_indices = []
            for j in range(len(column_name_list)):
                if i == j:
                    continue
                if column_name_list[i] == column_name_list[j]:
                    marked_indices.append(j)
            if len(marked_indices) > 0:
                marked_indices.append(i)
                for index in marked_indices:
                    if column_table_alias_list[index] is not None:
                        added_name = column_table_alias_list[index]
                    else:
                        added_name = column_table_name_list[index]
                    column_name_list[index] = added_name + "." + column_name_list[index]

        # print result
        width = []
        for col in column_name_list:
            w = len(col)
            for row in joined_rows:
                w = max(w, len(row[column_name_list.index(col)]))
            width.append(w)
        print("+" + "+".join(["-" * (w + 2) for w in width]) + "+")

        print("|" + "|".join([col.center(w + 2) for col, w in zip(column_name_list, width)]) + "|")
        print("+" + "+".join(["-" * (w + 2) for w in width]) + "+")

        for row in joined_rows:
            print("|" + "|".join([col.center(w + 2) for col, w in zip(row, width)]) + "|")
        print("+" + "+".join(["-" * (w + 2) for w in width]) + "+")

    # items[0] == Token "INSERT"
    # items[1] == Token "INTO"
    # items[2] == Tree "table_name"
    # items[3] == Tree "column_name_list"
    # items[4] == Token "VALUES"
    # items[5] == Tree "values"
    def insert_query(self, items):
        table_name = items[2].children[0].value
        tables = pickle.loads(catalogDB.get(b"tables"))
        # check if table exists
        if table_name not in tables:
            print(MY_PROMPT + "No such table")
            return
        table_info = pickle.loads(catalogDB.get(table_name.encode()))
        columns = table_info["columns"]
        targetDB = db.DB()
        targetDB.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH)

        query_col_list = None
        if items[3] is not None:
            query_col_list = items[3].children[1:-1]
        value_tree_list = items[5].children[1:-1]
        # check if column list is valid
        if query_col_list is not None:
            if len(query_col_list) != len(columns):
                print(MY_PROMPT + "Insertion has failed: Types are not matched")
                return
            for colTree in query_col_list:
                col_name = colTree.children[0].value.lower()
                if col_name not in columns:
                    print(MY_PROMPT + "Insertion has failed: '" + col_name + "' does not exist")
                    return

        PK_col_list = []
        PK_val_list = []
        FK_info = {}
        val_list = []
        for col_name, col_info in columns.items():
            if col_info["primary_key"]:
                PK_col_list.append(col_name)
                PK_val_list.append(None)
            if col_info["references"] is not None:
                FK_info[col_name] = col_info["references"]
            
        # check if value list is valid
        if len(value_tree_list) != len(columns):
            print(MY_PROMPT + "Insertion has failed: Types are not matched")
            return
        for i in range(len(value_tree_list)):
            # parse token_type, value, and col_info
            valueTree = value_tree_list[i].children[0]
            if isinstance(valueTree, str) and valueTree.lower() == "null":
                token_type = "NULL"
                value = "null"
            else:
                token_type = valueTree.children[0].type.lower()
                value = valueTree.children[0].value.lower()
            if query_col_list is not None:
                col_name = query_col_list[i].children[0].value.lower()
            else:
                col_name = list(columns.keys())[i]
            col_info = columns[col_name]

            # check if value is null but column is not nullable
            if value == "null" and not col_info["nullable"]:
                print(MY_PROMPT + "Insertion has failed: '" + col_name + "' is not nullable")
                return
            
            # check column type is valid
            if token_type == "int":
                if col_info["type"] != "int":
                    print(MY_PROMPT + "Insertion has failed: Types are not matched")
                    return
                try:
                    int_value = int(value)
                except ValueError:
                    print(MY_PROMPT + "Insertion has failed: Types are not matched")
                    return
            elif token_type == "str":
                if not col_info["type"].startswith("char"):
                    print(MY_PROMPT + "Insertion has failed: Types are not matched")
                    return
                value = value[1:-1]
                max_len = int(col_info["type"][5:-1])
                value = value[:max_len]
            elif token_type == "date":
                if col_info["type"] != "date":
                    print(MY_PROMPT + "Insertion has failed: Types are not matched")
                    return

            val_list.append(value)

            if col_info["primary_key"]:
                PK_val_list[PK_col_list.index(col_name)] = value

        # check if primary key is duplicated
        key_tuple = "*".join(PK_val_list).encode()
        if targetDB.exists(key_tuple):
            print(MY_PROMPT + "Insertion has failed: Primary key duplication")
            return
        
        # check if foreign key is valid
        FKV_list_by_table = {}
        for col_name, ref_info in FK_info.items():
            ref_table_name, ref_col_name = ref_info.split(".")
            if ref_table_name not in FKV_list_by_table:
                FKV_list_by_table[ref_table_name] = [val_list[list(columns.keys()).index(col_name)]]
            else:
                FKV_list_by_table[ref_table_name].append(val_list[list(columns.keys()).index(col_name)])

        for ref_table_name, FKV_list in FKV_list_by_table.items():
            refDB = db.DB()
            refDB.open('./DB/' + ref_table_name + '.db', dbtype=db.DB_HASH)
            key_tuple = "*".join(FKV_list).encode()
            if not refDB.exists(key_tuple):
                print(MY_PROMPT + "Insertion has failed: Referential integrity violation")
                return

        # Add row to table
        val_tuple = "*".join(val_list).encode()
        targetDB.put(key_tuple, val_tuple)

        print(MY_PROMPT + "The row is inserted")
        targetDB.close()

    # items[0] = TOKEN DELETE
    # items[1] = TOKEN FROM
    # items[2] = TREE table_name
    # items[3] = TREE where_clause
    def delete_query(self, items):
        table_name = items[2].children[0].value.lower()
        tables = pickle.loads(catalogDB.get(b"tables"))
        if table_name not in tables:
            print(MY_PROMPT + "No such table")
            return
        
        table_info = pickle.loads(catalogDB.get(table_name.encode()))
        table_columns = list(table_info["columns"].keys())
        targetDB = db.DB()
        targetDB.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH)
        rows = []
        will_be_deleted_keys = []
        for key, value in targetDB.items():
            value_list = value.decode().split("*")
            rows.append((key.decode(), value_list))
        
        # check where clause for each row
        if items[3] is not None:
            cols = []
            for col in table_columns:
                cols.append([table_name, None, col, None])

            for row in rows:
                test = test_bool_expr(cols, row[1], items[3].children[1])
                if test is True:
                    will_be_deleted_keys.append(row[0])
        else:
            for row in rows:
                will_be_deleted_keys.append(row[0])

        # check if foreign key is valid
        size = len(will_be_deleted_keys)
        for i in range(size):
            PK_values = will_be_deleted_keys[i].strip("'").split("*")

            ref_tables = table_info["referenced_by"]
            for ref_table_name in ref_tables:
                can_set_null = True
                ref_table_info = pickle.loads(catalogDB.get(ref_table_name.encode()))
                ref_table_columns = ref_table_info["columns"]
                for col_name, column_info in ref_table_columns.items():
                    if column_info["references"] is None:
                        continue
                    if column_info["references"].startswith(table_name) and column_info["nullable"] == False:
                        can_set_null = False
                        break
                
                if not can_set_null:
                    will_be_deleted_keys[i] = None

        # delete rows
        deleted_number = 0
        for key in will_be_deleted_keys:
            if key is not None:
                targetDB.delete(key.encode())
                pk_list = key.strip("'").split("*")
                for ref_table_name in ref_tables:
                    refDB = db.DB()
                    refDB.open('./DB/' + ref_table_name + '.db', dbtype=db.DB_HASH)
                    for ref_key, ref_value in refDB.items():
                        to_be_null = True
                        value_list = ref_value.decode().split("*")
                        for i in range(len(pk_list)):
                            if pk_list[i] not in value_list:
                                to_be_null = False
                                break
                        if to_be_null:
                            for i in range(len(pk_list)):
                                value_list[value_list.index(pk_list[i])] = "null"
                            refDB.put(ref_key, "*".join(value_list).encode())
                    refDB.close()

                deleted_number += 1

        targetDB.close()
        print(MY_PROMPT + str(deleted_number) + " row(s) are deleted")
    # items[0] = TOKEN UPDATE
    # items[1] = TREE table_name
    # items[2] = Token SET
    # items[3] = TREE column_name
    # items[4] = TOKEN COMP_OP
    # items[5] = TREE comparable_value
    # items[6] = TREE where_clause
    def update_query(self, items):
        table_name = items[1].children[0].value.lower()
        column_name = items[3].children[0].value.lower()
        value = items[5].children[0].value.lower()
        value_type = operand_type(value)

        tables = pickle.loads(catalogDB.get(b"tables"))
        if table_name not in tables:
            print(MY_PROMPT + "No such table")
            return
        
        table_info = pickle.loads(catalogDB.get(table_name.encode()))
        table_columns = list(table_info["columns"].keys())
        column_info = table_info["columns"][column_name]
        ref_tables = table_info["referenced_by"]
        # check if column exists
        if column_name not in table_columns:
            print(MY_PROMPT + "Update has failed: '" + column_name + "' does not exist")
            return
        # check type match
        if column_info["type"] == "int":
            try:
                int_value = int(value)
            except ValueError:
                print(MY_PROMPT + "Update has failed: Types are not matched")
                return
        elif column_info["type"] == "date":
            if value_type != "date":
                print(MY_PROMPT + "Update has failed: Types are not matched")
                return
        else:
            if value_type != "str":
                print(MY_PROMPT + "Update has failed: Types are not matched")
                return
            value = value[1:-1]
            max_len = int(column_info["type"][5:-1])
            value = value[:max_len]
        
        targetDB = db.DB()
        targetDB.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH)
        updated_num = 0
        not_updated_num = 0
        will_be_updated_list = []
        col_idx = table_columns.index(column_name)
        for key, val in targetDB.items():
            key_list = key.decode().split("*")
            val_list = val.decode().split("*")
            
            # check where clause for each row
            will_be_updated = False
            if items[6] is not None:
                cols = []
                for col in table_columns:
                    cols.append([table_name, None, col, None])
                test = test_bool_expr(cols, val_list, items[6].children[1])
                if test is True:
                    will_be_updated = True
            else:
                will_be_updated = True
                
            if will_be_updated:
                if column_info["primary_key"] == True:
                    # check primary key constraint
                    pk_list = table_info["pk_list"]
                    pk_idx = pk_list.index(column_name)
                    new_pk = key_list.copy()
                    new_pk[pk_idx] = value
                    new_pk = "*".join(new_pk)
                    if targetDB.has_key(new_pk.encode()):
                        print(MY_PROMPT + "Update has failed: Primary key duplication")
                        return
                    key_list = new_pk.split("*")
                    # check tables that reference this table
                    for ref_table_name in ref_tables:
                        can_set_null = True
                        ref_table_info = pickle.loads(catalogDB.get(ref_table_name.encode()))
                        ref_table_columns = ref_table_info["columns"]
                        for col_name, column_info in ref_table_columns.items():
                            if column_info["references"] is None:
                                continue
                            if column_info["references"].startswith(table_name) and column_info["nullable"] == False:
                                can_set_null = False
                                break
                        if not can_set_null:
                            not_updated_num += 1
                            will_be_updated = False
                            break
                # check tables that referenced by this table
                # append update list
                if will_be_updated:
                    will_be_updated_list.append([key_list, val_list, col_idx, value])
            
        # update rows
        for row in will_be_updated_list:
            key_list = row[0]
            val_list = row[1]
            col_idx = row[2]
            value = row[3]
            val_list[col_idx] = value
            targetDB.put("*".join(key_list).encode(), "*".join(val_list).encode())
            updated_num += 1
        targetDB.close()

        print(MY_PROMPT + str(updated_num) + " row(s) are updated")
        if not_updated_num != 0:
            print(MY_PROMPT + str(not_updated_num) + " row(s) are not updated due to referential integrity")

    def EXIT(self, items):
        raise SystemExit

# Helper functions for parsing where clause
def test_bool_expr(cols, vals, expr_tree):
    answer = False
    for i in range(0, len(expr_tree.children), 2):
        answer = answer or test_bool_term(cols, vals, expr_tree.children[i])
    return answer

def test_bool_term(cols, vals, term_tree):
    answer = True
    for i in range(0, len(term_tree.children), 2):
        answer = answer and test_bool_factor(cols, vals, term_tree.children[i])
    return answer

def test_bool_factor(cols, vals, factor_tree):
    if factor_tree.children[0] is None:
        return test_bool_test(cols, vals, factor_tree.children[1])
    else:
        return not test_bool_test(cols, vals, factor_tree.children[1])

def test_bool_test(cols, vals, test_tree):
    if len(test_tree.children) == 1:
        return test_predicate(cols, vals, test_tree.children[0])
    else:
        return test_bool_expr(cols, vals, test_tree.children[1])

def test_predicate(cols, vals, pred_tree):
    node = pred_tree.children[0]
    if node.data == "comparison_predicate":
        return test_comparison_predicate(cols, vals, node)
    elif node.data == "null_predicate":
        return test_null_predicate(cols, vals, node)

def parse_operand(cols, vals, operand_tree):
    # constant value
    if operand_tree.children[0] is not None and operand_tree.children[0].data == "comparable_value":
        if operand_tree.children[0].children[0].value.startswith("'"):
            return operand_tree.children[0].children[0].value[1:-1]
        return str(operand_tree.children[0].children[0].value)

    else:
        # no table name
        if operand_tree.children[0] is None:
            col_name = operand_tree.children[1].children[0].value.lower()
            if col_name not in [column[2] for column in cols]:
                raise Exception("WhereColumnNotExist")
            for i in range(len(cols)):
                if cols[i][2] == col_name:
                    return vals[i]

        # with table name
        else:
            table_name = operand_tree.children[0].children[0].value.lower()
            col_name = operand_tree.children[1].children[0].value.lower()
            for i in range(len(cols)):
                if cols[i][0] == table_name and cols[i][2] == col_name:
                    return vals[i]
                elif cols[i][1] == table_name and cols[i][2] == col_name:
                    return vals[i]

            if col_name in [column[2] for column in cols]:
                raise Exception("WhereTableNotSpecified")
            else:
                raise Exception("WhereColumnNotExist")

def operand_type(operand):
    # check if operand is int
    if operand.isdigit():
        return "int"
    # check if operand is date
    try:
        datetime.datetime.strptime(operand, '%Y-%m-%d')
        return "date"
    except ValueError:
        pass
    return "str"

def test_comparison_predicate(cols, vals, comp_tree):
    operand1 = parse_operand(cols, vals, comp_tree.children[0])
    operand2 = parse_operand(cols, vals, comp_tree.children[2])
    if operand_type(operand1) != operand_type(operand2):
        raise Exception("WhereIncomparableError")
    
    operator = comp_tree.children[1].value
    if operator == "=":
        return operand1 == operand2
    elif operator == ">":
        return operand1 > operand2
    elif operator == ">=":
        return operand1 >= operand2
    elif operator == "<":
        return operand1 < operand2
    elif operator == "<=":
        return operand1 <= operand2
    elif operator == "!=":
        return operand1 != operand2

def test_null_predicate(cols, vals, null_tree):
    table_name = ""
    if null_tree.children[0] is not None:
        table_name = null_tree.children[0].children[0].value.lower()
    col_name = null_tree.children[1].children[0].value.lower()
    if col_name not in [column[2] for column in cols]:
        raise Exception("WhereColumnNotExist")
    
    value = None
    if table_name == "":
        for i in range(len(cols)):
            if cols[i][2] == col_name:
                value = vals[i]
    else:
        for i in range(len(cols)):
            if cols[i][0] == table_name and cols[i][2] == col_name:
                value = vals[i]
            elif cols[i][1] == table_name and cols[i][2] == col_name:
                value = vals[i]
    null_operation = null_tree.children[2]
    if null_operation.children[1] is None:
        return value == "null"
    else:
        return value != "null"
# 쿼리 규칙
# 1. 쿼리는 항상 ;(세미콜론)으로 끝난다.
# 2. ;(세미콜론)이 나오기 전까지 개행문자를 받아도 쿼리를 끝내지 않는다. (대신 PROMPT는 출력되지 않음)
# 3. 한 줄에 여러 쿼리가 ;(세미콜론)으로 구분되어 들어오면, 처음부터 순차적으로 처리한다.
# 4. 쿼리에 오류가 없는 경우 (해당 쿼리) requestd를 출력하고, 에러가 발생한 경우 Syntax error를 출력한다.
def main():
    if os.path.exists("./DB/catalog.db"):
        catalogDB.open("./DB/catalog.db", dbtype=db.DB_HASH)
    else:
        catalogDB.open("./DB/catalog.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)
        catalogDB.put(b"tables", pickle.dumps(list()))

    with open("grammar.lark") as file:
        sql_parser = Lark(file.read(), start="command", parser="lalr", transformer=T())
    
    while(1):
        text = input(MY_PROMPT)
        while(";" not in text):
            text += " " + input()

        queries = [query.strip() + ";" for query in text.split(";") if query]
        for query in queries:
            try:
                sql_parser.parse(query)
            except exceptions.UnexpectedToken or exceptions.UnexpectedCharacters or exceptions.UnexpectedInput:
                print(MY_PROMPT + "Syntax error")
                break
            except Exception as e:
                err = e.args[0]
                if err == "DuplicateColumnDefError":
                    print(MY_PROMPT + "Create table has failed: column definition is duplicated")
                elif err == "DuplicatePrimaryKeyDefError":
                    print(MY_PROMPT + "Create table has failed: primary key definition is duplicated")
                elif err == "ReferenceTypeError":
                    print(MY_PROMPT + "Create table has failed: foreign key references wrong type")
                elif err == "ReferenceNonPrimaryKeyError":
                    print(MY_PROMPT + "Create table has failed: foreign key references non primary key column")
                elif err == "ReferenceColumnExistenceError":
                    print(MY_PROMPT + "Create table has failed: foreign key references non existing column")
                elif err == "ReferenceTableExistenceError":
                    print(MY_PROMPT + "Create table has failed: foreign key references non existing table")
                elif err.startswith("NonExistingColumnDefError"):
                    print(MY_PROMPT + "Create table has failed: '" + e.args[1] + "' does not exist in column definition")
                elif err == "TableExistenceError":
                    print(MY_PROMPT + "Create table has failed: table with the same name already exists")
                elif err == "WhrerColumnNotExist":
                    print(MY_PROMPT + "Where clause try to reference non existing column")
                elif err == "WhereTableNotSpecified":
                    print(MY_PROMPT + "Where clause try to reference tables which are not specified")
                elif err == "WhereIncomparableError":
                    print(MY_PROMPT + "Where clause try to compare incomparable values")
                elif err == "WhereAmbiguousReference":
                    print(MY_PROMPT + "Where clause contains ambiguous reference")
                else:
                    print(e)
                break
            except SystemExit:
                catalogDB.close()
                exit()


if __name__ == "__main__":
    main()