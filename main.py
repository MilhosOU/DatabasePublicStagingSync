import os
import psycopg2
from psycopg2 import sql

def copy_table_structure(cur, table_name):
    # Retrieve column definitions
    cur.execute(sql.SQL("""
        SELECT column_name, is_nullable, column_default, data_type, udt_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public' AND table_name = %s
    """), [table_name])

    columns = []
    for column in cur.fetchall():
        column_name, is_nullable, column_default, data_type, udt_name = column
        # Handle array data type
        if udt_name.startswith('_'):
            data_type = sql.SQL(" {}[]").format(sql.SQL(udt_name[1:]))
        else:
            data_type = sql.SQL(data_type)

        column_definition = sql.SQL("{} {}").format(
            sql.Identifier(column_name), 
            data_type
        )

        # Add NOT NULL if applicable
        if is_nullable == 'NO':
            column_definition += sql.SQL(" NOT NULL")

        # Check and append DEFAULT value if applicable
        if column_default:
            # Check for type casts in the default value
            if "::" in column_default:
                # Split the default value by the type cast operator
                default_value, cast_type = column_default.split("::", 1)
                # If the default value is a simple literal, quote it; otherwise, leave it as is
                if default_value.startswith("'") and default_value.endswith("'"):
                    column_definition += sql.SQL(" DEFAULT {}::{}").format(sql.Literal(default_value.strip("'")), sql.SQL(cast_type))
                else:
                    column_definition += sql.SQL(" DEFAULT {}::{}").format(sql.SQL(default_value), sql.SQL(cast_type))
            elif "nextval" in column_default or column_default.endswith("()"):
                # If default is a sequence or function, do not quote it
                column_definition += sql.SQL(" DEFAULT {}").format(sql.SQL(column_default))
            else:
                # Otherwise, treat as a literal value
                column_definition += sql.SQL(" DEFAULT {}").format(sql.Literal(column_default))

        columns.append(column_definition)

    # Combine all column definitions into a single SQL statement
    columns_sql = sql.SQL(', ').join(columns)
    create_table_sql = sql.SQL("""
        CREATE TABLE staging.{} ({})
    """).format(sql.Identifier(table_name), columns_sql)

    # Execute the CREATE TABLE statement
    cur.execute(create_table_sql)

    # Retrieve and recreate constraints
    cur.execute(sql.SQL("""
        SELECT conname, pg_get_constraintdef(pg_constraint.oid) 
        FROM pg_constraint 
        JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
        WHERE pg_class.relname = %s AND pg_constraint.contype IN ('p', 'f', 'u', 'c')
    """), [table_name])

    for constraint in cur.fetchall():
        constraint_name, constraint_def = constraint

        # Modify the constraint definition to fit the staging table
        # This might need adjustments depending on the constraint types and definitions
        constraint_sql = sql.SQL("ALTER TABLE staging.{} ADD CONSTRAINT {} {}").format(
            sql.Identifier(table_name),
            sql.Identifier(constraint_name),
            sql.SQL(constraint_def)
        )

        cur.execute(constraint_sql)

def copy_indexes(cur, table_name):
    cur.execute(sql.SQL("""
        SELECT indexdef FROM pg_indexes 
        WHERE tablename = %s AND schemaname = 'public' 
        AND indexname NOT IN (
            SELECT conname FROM pg_constraint 
            WHERE conrelid = 'public.{}'::regclass AND contype IN ('p', 'u')
        )
    """).format(sql.Identifier(table_name)), [table_name])

    for row in cur.fetchall():
        indexdef = row[0].replace(' ON public.', ' ON staging.')
        index_name = indexdef.split()[2]

        cur.execute(sql.SQL("""
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s AND n.nspname = 'staging'
        """), [index_name])

        if cur.fetchone() is None:
            cur.execute(sql.SQL(indexdef))

def copy_triggers(cur, table_name):
    cur.execute(sql.SQL("""
        SELECT pg_get_triggerdef(oid) 
        FROM pg_trigger 
        WHERE NOT tgisinternal AND tgrelid = 'public.{}'::regclass
    """).format(sql.Identifier(table_name)))

    for row in cur.fetchall():
        trigger_def = row[0].replace(' ON public.', ' ON staging.')
        cur.execute(sql.SQL(trigger_def))

def main():
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        host=os.getenv('DB_HOST')
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(sql.SQL("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public'
    """))
    tables = cur.fetchall()

    for table in tables:
        table_name = table[0]

        cur.execute(sql.SQL("""
            DROP TABLE IF EXISTS staging.{} CASCADE
        """).format(sql.Identifier(table_name)))

        copy_table_structure(cur, table_name)
        cur.execute(sql.SQL("""
            INSERT INTO staging.{} SELECT * FROM public.{}
        """).format(sql.Identifier(table_name), sql.Identifier(table_name)))

        copy_indexes(cur, table_name)
        copy_triggers(cur, table_name)

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
