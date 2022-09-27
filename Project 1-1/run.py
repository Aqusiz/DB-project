from lark import Lark, Transformer, exceptions

MY_PROMPT = "DB_2017-16140> "

class T(Transformer):
    def create_table_query(self, items):
        print("'CREATE TABLE' requested")
    
    def drop_table_query(self, items):
        print("'DROP TABLE' requested")

    def EXIT(self, items):
        raise SystemExit

# 쿼리 규칙
# 1. 쿼리는 항상 ;(세미콜론)으로 끝난다.
# 2. ;(세미콜론)이 나오기 전까지 개행문자를 받아도 쿼리를 끝내지 않는다. (대신 PROMPT는 출력되지 않음)
# 3. 한 줄에 여러 쿼리가 ;(세미콜론)으로 구분되어 들어오면, 처음부터 순차적으로 처리한다.
# 4. 쿼리에 오류가 없는 경우 (해당 쿼리) requestd를 출력하고, 에러가 발생한 경우 Syntax error를 출력한다.
def main():
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
            except exceptions.UnexpectedToken as e:
                print(e.token)
                print("Syntax error")
                break
            except SystemExit:
                exit()


if __name__ == "__main__":
    main()