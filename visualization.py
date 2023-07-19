# third party
import yaml
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# local imports
from connect import postgres_connection
import credentials

def create_data_visualization(df:pd.DataFrame) -> None:
    """
    This function receive the final dataframe and creates all the data visualization.

    Parameters:
        - df (pandas.Dataframe): the final dataframe
    """

    # Convert 'data_venda' to datetime if it's not already in the datetime format
    df['data_venda'] = pd.to_datetime(df['data_venda'])

    # Sort by data
    df.sort_values(by='data_venda', inplace=True)

    # Custom Y-axis formatter function
    def millions_format(x, pos):
        return f'{x/1e6:.0f}M'

    # Cumulative sales over time
    df['cumulative_sales'] = df['venda'].cumsum()
    plt.figure(figsize=(10, 6))
    plt.fill_between(df['data_venda'], df['cumulative_sales'], color='#0033cc')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Sales')
    plt.title('Cumulative Sales over Time')
    plt.xticks(rotation=45, ha='right')
    plt.grid(False)
    plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(millions_format))
    plt.tight_layout()
    plt.savefig('./graphs/1-cumulative_sales.png')
    plt.close()

    ## Categories comparative
    # Bar Chart
    df_sorted = df[['nome_categoria', 'venda']].groupby('nome_categoria').sum('venda').sort_values(by='venda', ascending=False)
    df_sorted.reset_index(inplace=True)
    plt.figure(figsize=(10, 10))
    plt.bar(df_sorted['nome_categoria'], df_sorted['venda'], color='#3b49ee')
    plt.xlabel('Category')
    plt.ylabel('Sales')
    plt.title('Sales by Category')
    plt.xticks(rotation=45, ha='right')
    plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(millions_format))
    plt.savefig('./graphs/2-bar_chart.png')
    plt.close()

    # Pie Chart
    plt.figure(figsize=(8, 8))
    plt.pie(
        df_sorted['venda'],
        labels=df_sorted['nome_categoria'],
        autopct='%1.1f%%',
        shadow=True,
        colors=['#3b49ee', '#777799', '#0033cc'])
    plt.title('Proportion of Sales by Category')
    plt.savefig('./graphs/3-pie_chart.png')
    plt.close()

    ## Employee Comparative
    # Bar Chart
    df_sorted = df.sort_values(by='venda', ascending=False)
    plt.figure(figsize=(10, 10))
    plt.bar(df_sorted['nome_funcionario'], df_sorted['venda'])
    plt.xlabel('Employee')
    plt.ylabel('Sales')
    plt.title('Sales by Employee')
    plt.xticks(rotation=45, ha='right')
    plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(millions_format))
    plt.savefig('./graphs/4-bar_chart_employee.png')
    plt.close()

if __name__ == '__main__':

    # load configurations
    with open('config.yaml') as config_file:
        config = yaml.safe_load(config_file)

    # connect with database
    connection = postgres_server_connection=postgres_connection(
        username=credentials.local_db_user,
        password=credentials.local_db_pass,
        database_name=config['Output']['database'],
        host=config['Output']['host'],
        port=config['Output']['port'],
    )

    df = pd.read_sql_query(config['Postgres']['query'], connection)
    create_data_visualization(df)