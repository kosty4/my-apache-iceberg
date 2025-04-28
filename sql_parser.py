

# Given a SQL statement, use SQLglot to parse the statement into a tree like object
# Parse the tree and give the parsed tree to an engine. 
# Let the engine decide what to do


#Build a catalog layer
#That holds some of the following metadata:

# Tables
# Schemas
# Locations

from dataclasses import dataclass, field
from typing import List, Optional, Union

import sqlglot
from sqlglot.expressions import Select, Insert, Table, Where, Column, Literal, Tuple


# insert into
# select
# update 
# delete

# Inserting data

# Parse a simple SQL query
# sql_select = "SELECT id, name FROM customers WHERE age > 30 ORDER BY name"

# Single row insert
# sql_insert = "INSERT INTO phone_book (name, number) VALUES ('John Doe', '555-1212')"

# Multi row insert
sql_insert = "INSERT INTO phone_book (name, number) VALUES ('John Doe', '555-1212'), ('Peter Doe', '555-2323');"

@dataclass
class ParsedInsert:
    target_table: str
    insert_columns: Optional[List[str]] = field(default_factory=list)
    insert_values: List[List[Union[str, int, float, None]]] = field(default_factory=list)

@dataclass
class ParsedSelect:
    source_table: str
    selected_columns: List[str] = field(default_factory=list)
    filter_clause: Optional[str] = None

class Parser:
    def process_select(self, parsed):

        from_ = parsed.args.get('from', '')
        where_ = parsed.args.get('where', '')

        selected_columns = []
        for proj in parsed.expressions:
            # Handle aliases, functions, etc.
            if isinstance(proj, Column):
                selected_columns.append(proj.name)
            else:
                selected_columns.append(proj.sql())

        print('parsed SELECT ', selected_columns, from_, where_)
        return ParsedSelect(source_tables=from_, selected_columns=selected_columns, filter_clause=where_)


    def process_insert(self, parsed):
        def process_row_vals(row_vals):
            values = []

            for val in row_vals:
                if isinstance(val, Literal):
                    values.append(val.this)
                else:
                    values.append(val.sql())
            return values

        target = parsed.args.get("this")

        #target table name
        target_table = target.this

        # columns to insert
        column_names = []
        for column in target.expressions:
            column_names.append(column.this)

        insert_expression = parsed.args.get("expression").expressions

        # column values
        column_values = []

        # Single (e.g., VALUES ('John Doe', '555-1212')) or
        # Multiple value rows (e.g., VALUES (...), (...), ...))
        for row in insert_expression:
            if isinstance(row, Tuple):
                row_vals = process_row_vals(row.expressions)
                column_values.append(row_vals)
        
        print('parsed INSERT ', target_table, column_names, column_values)

        return ParsedInsert(target_table=target_table, insert_columns=column_names, insert_values=column_values)


    def process_statement(self, sql_statement):

        parsed = sqlglot.parse_one(sql_statement)
        print(f"Expression type: {parsed.key}")

        print('parsed : ')
        print(repr(parsed))

        if isinstance(parsed, Select):
            return self.process_select(parsed)

        elif isinstance(parsed, Insert):
            return self.process_insert(parsed)

                

if __name__ == '__main__':

    parser = Parser()
    parser.process_statement(sql_insert)