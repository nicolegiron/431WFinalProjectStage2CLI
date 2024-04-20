import psycopg2

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname='Practice', 
            user='postgres', 
            password='1713', 
            host='localhost'
        )
        
        conn.autocommit = False
        return conn
    except Exception as e:
        print("An error occurred while connecting to the database:", e)
        return None

def execute_query(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        # Fetch result for SELECT queries
        if cur.description:
            rows = cur.fetchall()
            for row in rows:
                print(row)
        else:
            # Only print success message, do not commit here
            print("Query executed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        # Rollback is called when exception occurs
        conn.rollback()
    finally:
        cur.close()

def cond(query):
    condition = input("Enter a WHERE condition (e.g., 'id > 5') (Optional, press enter to skip): ")
    if condition:
        query += f" WHERE {condition}"
    return query

def cli():
    conn = connect_to_db()
    if conn is None:
        print("Failed to connect to the database. Exiting the program.")
        return
    try:
        while True:
            print("\nWelcome to the Database CLI Interface!\n")
            print("Please select an operation:")
            print("1. Insert Data")
            print("2. Delete Data")
            print("3. Update Data")
            print("4. Search Data")
            print("5. Aggregate Functions")
            print("6. Sorting")
            print("7. Joins")
            print("8. Grouping")
            print("9. Subqueries")
            print("10. Manage Transactions (Begin, Commit, Rollback)")
            print("11. Exit")

            choice = input("\nEnter your choice (1-11): ")
            try:
                choice = int(choice)
                if 1 <= choice <= 10:
                    if choice == 10:
                        transaction_command = input("Enter 'BEGIN', 'COMMIT', or 'ROLLBACK': ")
                        if transaction_command.upper() in ['BEGIN', 'COMMIT', 'ROLLBACK']:
                            execute_query(conn, transaction_command)
                        else:
                            print("Invalid transaction command.")
                    elif choice == 1:
                        table = input("\nEnter the table name you want to work with: ")
                        columns = input(f"Enter the columns for table {table} separated by commas: ")
                        values = input("Enter the values corresponding to the columns, separated by commas and in single quotes if string: ")
                        query = f"INSERT INTO {table} ({columns}) VALUES ({values})"
                        execute_query(conn, query)
                    elif choice == 2:
                        table = input("\nEnter the table name you want to work with: ")
                        query = f"DELETE FROM {table}"
                        query = cond(query)
                        execute_query(conn, query)
                    elif choice == 3:
                        table = input("\nEnter the table name you want to work with: ")
                        updates = input(f"Enter the update details for {table} (e.g., 'column1 = value1'): ")
                        query = f"UPDATE {table} SET {updates}"
                        query = cond(query)
                        execute_query(conn, query)
                    elif choice == 4:
                        table = input("\nEnter the table name you want to work with: ")
                        columns = input(f"Enter the columns to retrieve from {table}, separated by commas: ")
                        query = f"SELECT {columns} FROM {table}"
                        query = cond(query)
                        execute_query(conn, query)
                    elif choice == 5:
                        table = input("\nEnter the table name you want to work with: ")
                        function = input("Enter the aggregate function you want to use (SUM, AVG, COUNT, MIN, MAX): ")
                        column = input("Enter the column name to aggregate: ")
                        query = f"SELECT {function}({column}) FROM {table}"
                        query = cond(query)
                        execute_query(conn, query)
                    elif choice == 6:
                        table = input("\nEnter the table name you want to work with: ")
                        columns = input("Enter the columns you want to sort by, separated by commas: ")
                        order = input("Enter 'ASC' for ascending or 'DESC' for descending order: ")
                        query = f"SELECT * FROM {table}"
                        query = cond(query)
                        query += f" ORDER BY {columns} {order}"
                        execute_query(conn, query)
                    elif choice == 7:
                        table = input("\nEnter the table name you want to work with: ")
                        join_type = input("Enter the type of join (INNER, LEFT, RIGHT, FULL): ")
                        second_table = input("Enter the second table to join: ")
                        condition = input("Enter the join condition (e.g., 'table1.column1 = table2.column2'): ")
                        query = f"SELECT * FROM {table} {join_type} JOIN {second_table} ON {condition}"
                        query = cond(query)
                        execute_query(conn, query)
                    elif choice == 8:
                        table = input("\nEnter the table name you want to work with: ")
                        group_columns = input("Enter the column(s) you want to group by (separated by commas): ")
                        function = input("Enter the aggregate function you want to use (SUM, AVG, COUNT, MIN, MAX): ")
                        column = input("Enter the column name to aggregate: ")
                        query = f"SELECT {group_columns}, {function}({column}) FROM {table}"
                        query = cond(query)
                        query += f" GROUP BY {group_columns}"
                        execute_query(conn, query)
                    elif choice == 9:
                        table = input("\nEnter the table name you want to work with: ")
                        main_query = input("Enter the main query (e.g., 'SELECT * FROM table WHERE column IN'): ")
                        sub_query = input("Enter the sub query (e.g., 'SELECT * FROM table WHERE column IN'): ")
                        query = f"{main_query} ({sub_query})"
                        execute_query(conn, query)
                elif choice == 11:
                    print("Exiting the program.")
                    break
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
    finally:
        # Close the connection when the program ends
        if conn:
            conn.close()

if __name__ == "__main__":
    cli()
