#@title
import os
import openai
import duckdb
from openai import OpenAI

class QuackingDuck:

    def __init__(self, conn, client):
        self.client = client
        self.conn = conn
        self.schemas = self._get_schemas()

    def _get_schemas(self):
        tables = self.conn.execute("PRAGMA show_tables_expanded").fetchall()
        schemas = ""
        for table in tables:
            name = table[2]
            columns = [f"{table[3][i]}({table[4][i]})" for i in range(len(table[3]))]
            first_rows_md = self.conn.execute(f"SELECT * from {name} LIMIT 1;").fetchdf().to_markdown()
            schemas = schemas + f"{name} ({', '.join(columns)}):\n5 row sample:\n" + first_rows_md + "\n" + "\n"
        return schemas

    def explain_content(self, detail="one sentence"):
        print(self._schema_summary_internal(detail)[1])

    def _schema_summary_internal(self, detail="one sentence"):
        prompt = f"""SQL schema of my database:
        {self.schemas}
        Explain in {detail} what the data is about:
        """
        explanation = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that can generate an human redable summary of database content based on the schema."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        
        # ["choices"][0]["message"]["content"].strip("\n")
        res = explanation.choices[0].message.content.strip("\n")

        chat_completion = self.client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": "Say this is a test",
                }
            ],
            model="gpt-3.5-turbo",
        )

        return (prompt, res)

    def _generate_sql(self, question, debug=False):
        (summary_prompt, summary) = self._schema_summary_internal()
        sql_prompt = f"""Output a single SQL query without any explanation and do not add anything to the query that was not part of the question. Only if the question is not realted to the data in the database, answer with "I don't know".
        Make sure to only use tables and columns from the schema above and write a query to answer the following question:
        "{question}"
        """
        print('*********')
        print(summary)

        sql_query = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that can generate Postgresql code based on the user input. You do not respond with any human readable text, only SQL code."},
                {"role": "user", "content": summary_prompt},
                {"role": "assistant", "content": summary},
                {"role": "user", "content": sql_prompt},
            ],
            temperature=0,
        )

        res = sql_query.choices[0].message.content.strip("\n")
        res = res.strip('```sql')
        res = res.strip('```')
        
        # ["choices"][0]["message"]["content"].strip("\n")

        if debug:
            print("Prompt: \n"+sql_prompt)
            print("SQL Query: \n"+res)
        if "I don't know" in res:
            raise Exception("Question cannot be answered based on the data in the database.")

        return summary_prompt, summary, sql_prompt, res

    def _regenerate_sql(self, content_prompt, content_summary, sql_prompt, sql_query, error, debug=False):
        sql_query = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that can generate Postgresql code based on the user input. You do not respond with any human readable text, only SQL code."},
                {"role": "user", "content": content_prompt},
                {"role": "assistant", "content": content_summary},
                {"role": "user", "content": sql_prompt},
                {"role": "assistant", "content": sql_query},
                {"role": "user", "content": f"I got the following exception: {error}. Please correct the query and only print sql code, without an apology."},
            ],
            temperature=0,
        )

        res = sql_query.choices[0].message.content.strip("\n")
        
        # ["choices"][0]["message"]["content"].strip("\n")

        if debug:
            print("Corrected SQL Query: \n"+res)

        return res


    def ask(self, question, debug=False):
        summary_prompt, summary, sql_prompt, sql_query = self._generate_sql(question, debug)

        try:
            result = self.conn.execute(sql_query).fetchdf()
            result_markdown = result.head(10).to_markdown()
        except Exception as e:
            print("Query caused an error: " + str(e) + "\n Will try to fix it.\n")
            sql_query = self._regenerate_sql(summary_prompt, summary, sql_prompt, sql_query, str(e), debug)
            result = self.conn.execute(sql_query).fetchdf()
            result_markdown = result.head(10).to_markdown()

        answer_prompt = f"""Query Result:
        {result_markdown}
        Answer the question in natural language, based on information from the query result.
        """
        answer = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": summary_prompt},
                {"role": "assistant", "content": summary},
                {"role": "user", "content": sql_prompt},
                {"role": "assistant", "content": sql_query},
                {"role": "user", "content": answer_prompt},
            ],
            temperature=0,
        )

        res = answer.choices[0].message.content.strip("\n")

        # ["choices"][0]["message"]["content"].strip("\n")

        if debug:
            print("Prompt: \n"+answer_prompt)
            print("Answer: \n"+res)
        else:
            print(res)

        return result


print("foo")

#@title
con = duckdb.connect()
# Customers Table
con.execute('''
CREATE TABLE IF NOT EXISTS customers (
    customer_id char(10) NOT NULL,
    firstname varchar(32) default NULL,
    lastname varchar(32) default NULL,
    city varchar(32) default NULL,
    address varchar(128) default NULL,
    email varchar(128) default NULL,

    PRIMARY KEY (customer_id) )
''')

# Laptops Table
con.execute('''
CREATE TABLE IF NOT EXISTS laptops (
    model char(4) NOT NULL default '',
    speed double default NULL,
    ram int default NULL,
    hd int default NULL,
    screen double default NULL,
    price double default NULL,

    PRIMARY KEY  (model) )
''')

# PCs Table
con.execute('''
CREATE TABLE IF NOT EXISTS pcs (
    model char(4) NOT NULL,
    speed double NOT NULL,
    ram int NOT NULL,
    hd int NOT NULL,
    price double NOT NULL,

    PRIMARY KEY  (model) )
''')

# Printers Table
con.execute('''CREATE TABLE IF NOT EXISTS printers (
    model char(4) NOT NULL default '',
    color varchar(5) default NULL,
    type varchar(10) default NULL,
    price double default NULL,

    PRIMARY KEY  (model) )
''')

# Products Table
con.execute('''CREATE TABLE IF NOT EXISTS products (
    maker char(1) default NULL,
    model char(4) NOT NULL default '',
    type varchar(10) default NULL,

PRIMARY KEY  (model) )
''')

# Sales Table
con.execute('''CREATE TABLE IF NOT EXISTS sales (
    customer_id char(10) NOT NULL default '',
    model char(4) NOT NULL default '',
    quantity int default NULL,
    day date NOT NULL default '0000-00-00',
    paid double default NULL,
    type_of_payment varchar(32) default NULL,

    PRIMARY KEY  (customer_id,model,day) )
''')

# Customers Table
con.execute('''
INSERT INTO customers (customer_id, firstname, lastname, city, address, email)
VALUES
    ('1122334455', 'Ann', 'O''Brien', 'Rotterdam', '1 Jervis St.', 'ann@uva.nl'),
    ('1231231231', 'John', 'Doe', 'Amsterdam', NULL, NULL),
    ('1234567890', 'Maya', 'Ramanath', 'Diemen', 'Dalsteindreef 3002', NULL),
    ('9876543210', 'Ji', 'Zhang', 'Utrecht', 'Jaarbeursplien 24', 'jack@ucc.ie'),
    ('9999999999', 'Norah', 'Jones', 'Amsterdam', 'Passheuvelweg 34', 'nj@yahoo.com');
''')

# Laptops Table
con.execute('''
INSERT INTO laptops (model, speed, ram, hd, screen, price)
VALUES
    ('2001', 2, 2048, 240, 20.1, 3673),
    ('2002', 1.73, 1024, 80, 17, 949),
    ('2003', 1.8, 512, 60, 15.4, 549),
    ('2004', 2, 512, 60, 13.3, 1150),
    ('2005', 2.16, 1024, 120, 17, 2500),
    ('2006', 2, 2048, 80, 15.4, 1700),
    ('2007', 1.83, 1024, 120, 13.3, 1429),
    ('2008', 1.6, 1024, 100, 15.4, 900),
    ('2009', 1.6, 512, 80, 14.1, 680),
    ('2010', 2, 2048, 160, 15.4, 2300);
''')

# PCs Table
con.execute('''
INSERT INTO pcs (model, speed, ram, hd, price)
VALUES
    ('1001', 2.66, 1024, 250, 2114),
    ('1002', 2.1, 512, 250, 995),
    ('1003', 1.42, 512, 80, 478),
    ('1004', 2.8, 1024, 250, 649),
    ('1005', 3.2, 512, 250, 630),
    ('1006', 3.2, 1024, 320, 1049),
    ('1007', 2.2, 1024, 200, 510),
    ('1008', 2.2, 2048, 250, 770),
    ('1009', 2, 1024, 250, 650),
    ('1010', 2.8, 2048, 300, 770),
    ('1011', 1.86, 2048, 160, 959),
    ('1012', 2.8, 1024, 160, 649),
    ('1013', 3.06, 512, 80, 529);
''')

# Printers Table
con.execute('''
INSERT INTO printers (model, color, type, price)
VALUES
    ('3001', 'TRUE', 'ink-jet', 99),
    ('3002', 'FALSE', 'laser', 239),
    ('3003', 'TRUE', 'laser', 899),
    ('3004', 'TRUE', 'ink-jet', 120),
    ('3005', 'FALSE', 'laser', 120),
    ('3006', 'TRUE', 'ink-jet', 100),
    ('3007', 'TRUE', 'laser', 200);
''')

# Products Table
con.execute('''
INSERT INTO products (maker, model, type)
VALUES
    ('A', '1001', 'pc'),
    ('A', '1002', 'pc'),
    ('A', '1003', 'pc'),
    ('B', '1004', 'pc'),
    ('B', '1005', 'pc'),
    ('B', '1006', 'pc'),
    ('C', '1007', 'pc'),
    ('D', '1008', 'pc'),
    ('D', '1009', 'pc'),
    ('D', '1010', 'pc'),
    ('E', '1011', 'pc'),
    ('E', '1012', 'pc'),
    ('E', '1013', 'pc'),
    ('E', '2001', 'laptop'),
    ('E', '2002', 'laptop'),
    ('E', '2003', 'laptop'),
    ('A', '2004', 'laptop'),
    ('A', '2005', 'laptop'),
    ('A', '2006', 'laptop'),
    ('B', '2007', 'laptop'),
    ('F', '2008', 'laptop'),
    ('F', '2009', 'laptop'),
    ('G', '2010', 'laptop'),
    ('E', '3001', 'printer'),
    ('E', '3002', 'printer'),
    ('E', '3003', 'printer'),
    ('D', '3004', 'printer'),
    ('D', '3005', 'printer'),
    ('H', '3006', 'printer'),
    ('H', '3007', 'printer');
''')

# Sales Table
con.execute('''
INSERT INTO sales (customer_id, model, quantity, day, paid, type_of_payment)
VALUES
    ('1122334455', '2010', 1, '2020-12-19', 2300, 'mastercard credit'),
    ('1122334455', '3001', 1, '2020-12-18', 99, 'cash'),
    ('1231231231', '2002', 2, '2020-12-19', 1898, 'visa credit'),
    ('1231231231', '3002', 1, '2020-12-18', 239, 'cash'),
    ('1234567890', '1001', 1, '2020-12-20', 1902.6, 'mastercard credit'),
    ('9876543210', '1007', 1, '2020-12-17', 510, 'visa debit'),
    ('9876543210', '1007', 3, '2020-12-19', 1530, 'visa debit'),
    ('9876543210', '2002', 1, '2020-12-17', 949, 'visa debit'),
    ('9999999999', '1007', 1, '2020-12-20', 459, 'visa credit'),
    ('9999999999', '3007', 2, '2020-12-20', 360, 'visa credit');
''')

client = OpenAI(
    # This is the default and can be omitted
    # api_key=os.environ.get("OPENAI_API_KEY"),
)
quack = QuackingDuck(con, client)

quack.ask("Who bought the most PCs, print also the users name?", debug=True)
