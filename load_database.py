import json

def create_table(cursor):
  cursor.execute('DROP TABLE IF EXISTS data;')

  cursor.execute('''
    CREATE TABLE data (
      id SERIAL,
      a integer NOT NULL,
      b integer NOT NULL
    );
  ''')

def load_database(cursor):
  # Cria tabela data
  create_table (cursor)

  # Abre arquivo de metadados apenas para leitura
  file = open('test_files/metadado.json', 'r')

  try:
    # Pega dados do arquivo
    data = json.load(file)['INITIAL']
    tuples = list( zip(data['A'], data['B']) )

    # Percorre tuplas
    for tuple in tuples:
      # Converte dados das tuplas para string
      tuple = [str(column) for column in tuple]
      # Cria string de valor das colunas separadas por vírgula
      values = ', '.join(tuple)
      # Insere tupla na tabela
      insert_query = 'INSERT INTO data(a, b) VALUES (' + values + ')'
      cursor.execute(insert_query)

  finally:
    # Fecha arquivo
    file.close()
