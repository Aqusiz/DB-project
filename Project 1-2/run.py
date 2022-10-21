# python 3.9.6
# berkeley-db 5.3.28
# pip berkeleydb 18.1.5
from lark import Lark, Transformer, exceptions
from berkeleydb import db
import pickle
import os

MY_PROMPT = "DB_2017-16140> "
catalogDB = db.DB()
targetDB = db.DB()

class TreeParser():
    def parse(self, root):
        if root.data == "table_element_list":
            return self.parse_table_element_list(root)
    
    def parse_table_element_list(self, root):
        col_dict = {}
        col_defs = root.find_data("column_definition")
        pk_constraint = list(root.find_data("primary_key_constraint"))
        ref_constraint = root.find_data("referential_constraint")
        # parse column definitions
        for col_def in col_defs:
            col_name, col_type, col_nullable = self.parse_column_definition(col_def)
            if col_name in col_dict:
                raise Exception("DuplicateColumnDefError")
            col_dict[col_name] = {
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
            if pk not in col_dict:
                raise Exception("NonExistingColumnDefError("+pk+")")
            col_dict[pk]["nullable"] = False
            col_dict[pk]["primary_key"] = True
        # parse foreign key constraint
        for ref_con in ref_constraint:
            ref_name, ref_table_name, ref_col_name = self.parse_referential_constraint(ref_con)
            if ref_name not in col_dict:
                raise Exception("NonExistingColumnDefError("+ref_name+")")
            col_dict[ref_name]["references"] = ref_table_name+"."+ref_col_name

        return col_dict

    def parse_column_definition(self, root):
        col_name = root.children[0].children[0].value
        col_type = "".join(x.value for x in root.children[1].children)
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
            column_name = column.children[0].value
            if column_name in pk_list:
                raise Exception("DuplicatePrimaryKeyDefError")
            else:
                pk_list.append(column_name)
        
        return pk_list

    def parse_referential_constraint(self, root):
        
        return col_name, ref_table_name, ref_col_name

class T(Transformer):
    # items[0] == Token "CREATE"
    # items[1] == Token "TABLE"
    # items[2] == Tree "table_name"
    # items[3] == Tree "table_element_list"
    def create_table_query(self, items):
        table_name = items[2].children[0].value
        tables = pickle.loads(catalogDB.get(b"tables"))
        if table_name in tables:
            raise Exception("TableExistenceError")
        tables.append(table_name)

        col_dict = TreeParser().parse(items[3])

        catalogDB.put(b"tables", pickle.dumps(tables))
        catalogDB.put(table_name.encode(), pickle.dumps(col_dict))

        print(pickle.loads(catalogDB.get(b"tables")))
        print(pickle.loads(catalogDB.get(table_name.encode())))
        
    # items[0] == Token "DROP"
    # items[1] == Token "TABLE"
    # items[2] == Tree "table_name"
    def drop_table_query(self, items):
        print(MY_PROMPT + "'DROP TABLE' requested")

    def desc_query(self, items):
        print(MY_PROMPT + "'DESC' requested")

    def show_tables_query(self, items):
        print(MY_PROMPT + "'SHOW TABLES' requested")

    def select_query(self, items):
        print(MY_PROMPT + "'SELECT' requested")

    def insert_query(self, items):
        print(MY_PROMPT + "'INSERT' requested")

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
    '''
    if os.path.exists("./DB/catalog.db"):
        catalogDB.open("./DB/catalog.db", dbtype=db.DB_HASH)
    else:
        catalogDB.open("./DB/catalog.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)
        catalogDB.put(b"tables", pickle.dumps(list()))
    '''
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
            except SystemExit:
                catalogDB.close()
                exit()


if __name__ == "__main__":
    main()