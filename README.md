# Desafio Técnico

O desafio técnico vai consistir na solução da seguinte problemática, temos três fontes com seguintes dados:

- Banco PostgreSQL com dados de vendas
- API com dados de funcionários
- Arquivo parquet com dados de categoria

O objetivo desse desafio é criar um pipeline para movimentar esses dados para um banco de dados no seu ambiente, na estrutura que você achar melhor, considerando que ao fim vamos ter as vendas, funcionários e categorias em um só lugar. (Nossa sugestão é usar Python para ingerir e tratar os dados e PostgreSQL como banco de dados)

Um requisito desse desafio é que essa movimentação de dados seja feita diariamente, pois foi informado que todas as fontes recebem dados novos periodicamente e assim os dados no seu ambiente vão ficar atualizados, considerando isso, é importante que tenha um orquestrador para acionar o seu pipeline automaticamente. (Nossa sugestão é usar Apache Airflow).

Para ficar mais claro, segue uma imagem da arquitetura desejada:

![challenge](./readme_images/chanllenge.png/)