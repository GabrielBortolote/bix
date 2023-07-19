# third party imports
import yaml
import unittest

# local imports
import credentials
from connect import postgres_connection

# Import the functions to test (assuming they are in the same module)
from fetchers import fetch_data, fetch_names, fetch_categories

class TestYourFunctions(unittest.TestCase):

    def setUp(self):
        # load configurations
        with open('config.yaml') as config_file:
            self.config = yaml.safe_load(config_file)

    def tearDown(self):
        # Clean up any resources used during the tests here.
        pass

    # Test fetch_data function
    def test_fetch_data(self):
        
        # create connection
        connection = postgres_connection(
            username=credentials.db_user,
            password=credentials.db_pass,
            database_name=self.config['Postgres']['database'],
            host=self.config['Postgres']['host'],
            port=self.config['Postgres']['port'],
        )

        # call function
        df = fetch_data(connection, 'SELECT * FROM public.venda;')

        # assert columns
        columns = list(df.columns)
        columns.sort()
        expected = ['data_venda', 'id_categoria', 'id_funcionario', 'id_venda', 'venda']
        assert columns == expected, 'Query return unexpected columns'


    # Test fetch_names function
    def test_fetch_names(self):
        
        df = fetch_names(ids=[1], base_url=self.config['Urls']['api'])
        assert df.shape == (1, 2), 'Unexpected dataframe dimensions'
        assert list(df.columns) == ['id_funcionario', 'nome_funcionario'], 'Unexpected columns'

    # Test fetch_categories function
    def test_fetch_categories(self):
       
       df = fetch_categories(self.config['Urls']['parquet'])
       columns = list(df.columns)
       columns.sort()
       assert columns == ['id_categoria', 'nome_categoria'], 'Unexpected columns'

if __name__ == '__main__':
    unittest.main()