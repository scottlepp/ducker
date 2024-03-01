#@title
import os
import openai
import duckdb
from openai import OpenAI

class QuackingDuck:

    def __init__(self, client):
        self.client = client

    def _get_schemas(self):
        tables = self.conn.execute("PRAGMA show_tables_expanded").fetchall()
        schemas = ""
        for table in tables:
            name = table[2]
            columns = [f"{table[3][i]}({table[4][i]})" for i in range(len(table[3]))]
            first_rows_md = self.conn.execute(f"SELECT * from {name} LIMIT 1;").fetchdf().to_markdown()
            schemas = schemas + f"{name} ({', '.join(columns)}):\n5 row sample:\n" + first_rows_md + "\n" + "\n"
        print("********")
        print(schemas)
        return schemas

    def explain_content(self, detail="one sentence"):
        print(self._schema_summary_internal(detail)[1])

    def _schema_summary_internal(self, schema, detail="one sentence"):
        prompt = f"""SQL schema of my database:
        {schema}
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

    def _generate_sql(self, question, schema, debug=False):
        (summary_prompt, summary) = self._schema_summary_internal(schema)
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


    def ask(self, question, schema, debug=False):
        summary_prompt, summary, sql_prompt, sql_query = self._generate_sql(question, schema, debug)

        return sql_query
