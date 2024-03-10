import os
import psycopg2
from psycopg2 import sql

def copy_table_structure(cur, table_name):
    cur.execute(sql.SQL("""
        CREATE TABLE staging.{} AS TABLE public.{} WITH NO DATA
    """).format(sql.Identifier(table_name), sql.Identifier(table_name)))

    cur.execute(sql.SQL("""
        SELECT conname, pg_get_constraintdef(oid) 
        FROM pg_constraint 
        WHERE conrelid = 'public.{}'::regclass AND contype != 'i'
    """).format(sql.Identifier(table_name)))

    for row in cur.fetchall():
        constraint_name, constraint_def = row
        if 'PRIMARY KEY' in constraint_def:
            cur.execute(sql.SQL("""
                ALTER TABLE staging.{} ADD PRIMARY KEY {}
            """).format(sql.Identifier(table_name), 
                        sql.SQL(constraint_def.split("PRIMARY KEY")[1])))
        else:
            cur.execute(sql.SQL("""
                ALTER TABLE staging.{} ADD CONSTRAINT {} {}
            """).format(sql.Identifier(table_name), sql.Identifier(constraint_name),
                        sql.SQL(constraint_def)))

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
