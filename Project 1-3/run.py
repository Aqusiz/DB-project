# python 3.9.6
# berkeley-db 5.3.28
# pip berkeleydb 18.1.5
from lark import Lark, Transformer, exceptions
from berkeleydb import db
import pickle
import os

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

    def select_query(self, items):
        print(MY_PROMPT + "'SELECT' requested")

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
        print("check if table exists")
        if table_name not in tables:
            print(MY_PROMPT + "No such table")
            return
        print("table exists")
        table_info = pickle.loads(catalogDB.get(table_name.encode()))
        columns = table_info["columns"]
        targetDB = db.DB()
        targetDB.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH)

        query_col_list = None
        if items[3] is not None:
            query_col_list = items[3].children[1:-1]
        value_tree_list = items[5].children[1:-1]
        print(value_tree_list)
        # check if column list is valid
        print("check if column list is valid")
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
        FK_col_list = []
        PK_val_list = []
        FK_val_list = []
        val_list = []
        for col_name, col_info in columns.items():
            if col_info["primary_key"]:
                PK_col_list.append(col_name)
                PK_val_list.append(None)
            if col_info["references"] is not None:
                FK_col_list.append(col_name)
                FK_val_list.append(None)
            
        # check if value list is valid
        print("check if value list is valid")
        if len(value_tree_list) != len(columns):
            print(MY_PROMPT + "Insertion has failed: Types are not matched")
            return
        for i in range(len(value_tree_list)):
            # parse token_type, value, and col_info
            print("parse token_type, value, and col_info")
            valueTree = value_tree_list[i].children[0]
            if isinstance(valueTree, str) and valueTree.lower() == "null":
                token_type = "NULL"
                value = "null"
            else:
                token_type = valueTree.children[0].type.lower()
                value = valueTree.children[0].value.lower()
            print(token_type)
            print(value)
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

            #if col_info["references"] is not None:
            #    FK_val_list[FK_col_list.index(col_name)] = value

        # check if primary key is duplicated
        key_tuple = "*".join(PK_val_list).encode()
        if targetDB.exists(key_tuple):
            print(MY_PROMPT + "Insertion has failed: Primary key duplication")
            return

        val_tuple = "*".join(val_list).encode()
        targetDB.put(key_tuple, val_tuple)

        print(MY_PROMPT + "The row is inserted")

    def delete_query(self, items):
        print(MY_PROMPT + "'DELETE' requested")

    def update_query(self, items):
        print(MY_PROMPT + "'UPDATE' requested")

    def EXIT(self, items):
        raise SystemExit

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
                else:
                    print(e)
                break
            except SystemExit:
                catalogDB.close()
                exit()


if __name__ == "__main__":
    main()