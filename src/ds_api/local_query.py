from db_api import read, write

"""
Instructions for using this module:

1. Use the `read` function to execute SELECT queries.
   - The result will be printed in a table format by default.
   - To suppress the table output, pass `verbose=False` as an argument.

2. Use the `write` function for executing INSERT, UPDATE, DELETE, or other non-SELECT SQL statements.

3. Note: The current user 'ds_user' does not have permissions to modify the table structure (e.g., ALTER TABLE). 
   Ensure you have the necessary permissions or consult the database administrator before attempting structural changes.

Example usage:

query = "INSERT INTO users (user_id, ...) VALUES (...);"
write(query)

data_frame = read("SELECT * FROM users WHERE ...;")

"""

query = """
    INSERT INTO users (user_id, birth_date, subscription_date, subscription_type) 
    VALUES ('999', '2020-01-01', '2021-01-01', 'premium');
    """
write(query)

read("""
     SELECT * 
     FROM users
     WHERE user_id IN ('999');
     """)

query = """
    DELETE FROM users WHERE user_id = '999';
    """
write(query)
