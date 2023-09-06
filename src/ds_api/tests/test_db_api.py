import unittest
from ds_api.db_api import read, write

class TestDBAPI(unittest.TestCase):

    def setUp(self):
        """Setup mock data for testing."""
        self.insert_query = """
        INSERT INTO users (user_id, birth_date, subscription_date, subscription_type) 
        VALUES ('999', '2020-01-01', '2021-01-01', 'premium');
        """
        write(self.insert_query)

    def tearDown(self):
        """Cleanup test data after each test."""
        delete_query = """
        DELETE FROM users WHERE user_id = '999';
        """
        write(delete_query)

    def test_read_function(self):
        """Test the read function."""
        select_query = """
        SELECT * 
        FROM users
        WHERE user_id = '1';
        """
        df = read(select_query)
        self.assertEqual(df.iloc[0]['user_id'], 1)

    def test_write_function(self):
        """Test the write function."""
        # Deleting and then adding the data back to test the write function
        delete_query = """
        DELETE FROM users WHERE user_id = '999';
        """
        write(delete_query)
        
        select_query = """
        SELECT * 
        FROM users
        WHERE user_id IN ('999');
        """
        df = read(select_query, verbose=False) # Using verbose=False to suppress the table output
        self.assertTrue(df.empty) # After deletion, no data should be returned
        
        write(self.insert_query)
        df = read(select_query, verbose=False)
        self.assertFalse(df.empty) # After insertion, data should be returned

if __name__ == "__main__":
    unittest.main()
