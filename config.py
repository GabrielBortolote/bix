config = {
    'local': {
        'host': 'localhost',
        'port': '5432',
        'dbname': 'bix_challenge',
    },
    'server': {
        'host': '34.173.103.16',
        'port': '5432',
        'dbname': 'postgres',
    },
    'postgres_query': 'SELECT * FROM public.venda;',
    'api_url': 'https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior',
    'parquet_url': 'https://storage.googleapis.com/challenge_junior/categoria.parquet',
}