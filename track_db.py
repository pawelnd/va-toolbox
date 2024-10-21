import psycopg2
import psycopg2.extras
import struct
import os
import platform
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Dictionary to map relation IDs to table names and columns
relation_map = {}

# Define connection details from environment variables
conn_params = {
    'dbname': os.getenv('DB_NAME'),      # Database name loaded from .env
    'user': os.getenv('DB_USER'),        # PostgreSQL username from .env
    'password': os.getenv('DB_PASSWORD'),# PostgreSQL password from .env
    'host': os.getenv('DB_HOST'),        # PostgreSQL host from .env
    'port': os.getenv('DB_PORT'),        # PostgreSQL port from .env
}

# Function to populate the relation_map with relation id, table names, and columns
def fetch_relation_map():
    global relation_map
    try:
        with psycopg2.connect(**conn_params) as metadata_conn:
            with metadata_conn.cursor() as cursor:
                query = """
                SELECT c.oid, n.nspname, c.relname,
                       array_agg(a.attname ORDER BY a.attnum) AS column_names
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_attribute a ON a.attrelid = c.oid
                WHERE c.relkind IN ('r', 'p')  -- 'r' for regular tables, 'p' for partitioned tables
                  AND a.attnum > 0  -- Exclude system columns
                  AND NOT a.attisdropped  -- Exclude dropped columns
                GROUP BY c.oid, n.nspname, c.relname
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                for oid, schema, table, columns in rows:
                    relation_map[oid] = {
                        'table_name': f"{schema}.{table}",
                        'columns': columns
                    }

    except psycopg2.Error as e:
        print("Error fetching relation map:", e)


def clear_console():
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')


# Make a replication connection
try:
    # Make sure to use `ReplicationConnection` for logical replication
    conn = psycopg2.connect(
        dbname=conn_params['dbname'],
        user=conn_params['user'],
        password=conn_params['password'],
        host=conn_params['host'],
        port=conn_params['port'],
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
        replication=psycopg2.extras.REPLICATION_LOGICAL
    )
    print("Replication connection established")
except psycopg2.Error as e:
    print("Error connecting to PostgreSQL replication:", e)
    exit(1)


# Function to decode logical replication messages
def decode_pgoutput_message(payload):
    message_type = payload[0:1].decode('ascii')
    if payload[0:1].decode('ascii') == 'B':  # Transaction BEGIN (optional)
        return

    if message_type == 'I':  # INSERT message
        print("----------------")
        print("INSERT operation detected.")
        relation_id, = struct.unpack('!I', payload[1:5])  # Decode relation OID (4 bytes)
        relation_info = relation_map.get(relation_id, None)
        if relation_info:
            table_name = relation_info['table_name']
            columns = relation_info['columns']
        else:
            table_name = 'Unknown relation'
            columns = []
        print(f"Insert into table: {table_name}")

        rest = payload[5:]

        if rest[0:1].decode('ascii') == 'N':
            column_count = struct.unpack('!H', rest[1:3])[0]  # Number of columns (2-byte integer)
            offset = 3
            print(f"Number of columns in insertion: {column_count}")
            for col_idx in range(column_count):
                isnull = rest[offset:offset+1]  # Nullable flag
                col_name = columns[col_idx] if col_idx < len(columns) else f"unnamed_col_{col_idx + 1}"
                if isnull == b't':
                    offset += 1
                    col_len = struct.unpack('!I', rest[offset:offset+4])[0]
                    offset += 4
                    col_value = rest[offset:offset+col_len].decode('utf-8')
                    offset += col_len
                    print(f"{col_name}: {col_value}")
                else:
                    offset += 1
                    print(f"{col_name}: NULL")
        return

    elif message_type == 'U':  # UPDATE message
        print("----------------")
        print("UPDATE operation detected.")
        relation_id, = struct.unpack('!I', payload[1:5])  # Decode relation OID (4 bytes)
        relation_info = relation_map.get(relation_id, None)
        if relation_info:
            table_name = relation_info['table_name']
            columns = relation_info['columns']
        else:
            table_name = 'Unknown relation'
            columns = []
        print(f"Update in table: {table_name}")
        rest = payload[5:]

        if rest[0:1].decode('ascii') == 'K':
            print("Key tuple (identifying row before update) follows.")
            rest = rest[1:]

        if rest[0:1].decode('ascii') == 'N':
            print("New tuple values after update:")
            rest = rest[1:]
            column_count = struct.unpack('!H', rest[0:2])[0]
            offset = 2
            print(f"Number of columns in update: {column_count}")
            for col_idx in range(column_count):
                isnull = rest[offset:offset+1]  # Nullable flag
                col_name = columns[col_idx] if col_idx < len(columns) else f"unnamed_col_{col_idx + 1}"
                if isnull == b't':
                    offset += 1
                    col_len = struct.unpack('!I', rest[offset:offset+4])[0]
                    offset += 4
                    col_value = rest[offset:offset+col_len].decode('utf-8')
                    offset += col_len
                    print(f"{col_name}: {col_value}")
                else:
                    offset += 1
                    print(f"{col_name}: NULL")

        return

    elif message_type == 'D':  # DELETE message
        print("----------------")
        print("DELETE operation detected.")
        relation_id, = struct.unpack('!I', payload[1:5])
        relation_info = relation_map.get(relation_id, None)
        if relation_info:
            table_name = relation_info['table_name']
            columns = relation_info['columns']
        else:
            table_name = 'Unknown relation'
            columns = []
        print(f"Delete from table: {table_name}")
        rest = payload[5:]

        if rest[0:1].decode('ascii') == 'K':
            print("Key tuple (identifying row to delete) follows.")
            column_count = struct.unpack('!H', rest[1:3])[0]
            offset = 3
            print(f"Number of columns in key tuple: {column_count}")
            for col_idx in range(column_count):
                isnull = rest[offset:offset+1]
                col_name = columns[col_idx] if col_idx < len(columns) else f"unnamed_col_{col_idx + 1}"
                if isnull == b't':
                    offset += 1
                    col_len = struct.unpack('!I', rest[offset:offset+4])[0]
                    offset += 4
                    col_value = rest[offset:offset+col_len].decode('utf-8')
                    offset += col_len
                    print(f"{col_name}: {col_value}")
                else:
                    offset += 1
                    print(f"{col_name}: NULL")
        return


# Stream changes from PostgreSQL replication
def stream_changes():
    with conn.cursor() as cur:
        print("Starting logical streaming replication...")

        # Fetch the OID to table relation mapping before starting replication
        fetch_relation_map()

        # Start replication from logical slot
        cur.start_replication(
            slot_name=os.getenv('REPLICATION_SLOT'),  # Slot name from .env
            options={
                'proto_version': '1',
                'publication_names': os.getenv('PUBLICATION_NAME')  # Publication name from .env
            }
        )

        def consume_change(msg):
            if msg:
                decode_pgoutput_message(msg.payload)
                msg.cursor.send_feedback(flush_lsn=msg.data_start)

        try:
            cur.consume_stream(consume_change)
        except KeyboardInterrupt:
            print("\nStopping replication stream...")


try:
    stream_changes()
except Exception as e:
    print(f"Error during replication: {e}")
finally:
    # Close PostgreSQL connection
    conn.close()
