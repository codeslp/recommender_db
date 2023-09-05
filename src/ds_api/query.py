from db_api import read, write

"""
You may pass the parameter verbose=False to query_db() to suppress the output.
Right now ds_user does not have permission to create alter table structure.
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