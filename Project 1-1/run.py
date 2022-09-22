from lark import Lark, Transformer

MY_PROMPT = "DB_2017-16140> "

class T(Transformer):
    def create_table_query(self, items):
        return

# 쿼리 규칙
# 1. 쿼리는 항상 ;(세미콜론)으로 끝난다.
# 2. ;(세미콜론)이 나오기 전까지 개행문자를 받아도 쿼리를 끝내지 않는다. (대신 PROMPT는 출력되지 않음)
# 3. 한 줄에 여러 쿼리가 ;(세미콜론)으로 구분되어 들어오면, 처음부터 순차적으로 처리한다.
# 4. 쿼리에 오류가 없는 경우 (해당 쿼리) requestd를 출력하고, 에러가 발생한 경우 Syntax error를 출력한다.
def main():
    with open("grammar.lark") as file:
        sql_parser = Lark(file.read(), start="command", lexer="basic")
    
    while(1):
        queries = []
        query = input(MY_PROMPT)
        while(";" not in query):
            text = input()
            query += text
        queries = [q.strip() for q in query.split(";")]


if __name__ == "__main__":
    main()