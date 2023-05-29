# Pacote do regex
import re

# Scripts para impressão
from scripts.print_out import print_redo_transactions, print_undo_transactions, print_json, print_update

#não será usado
""" def find_checkpoint(file):
  # Encontrando o checkpoint e salvando as transações que ainda não terminaram
  matches = re.findall('<CKPT \((.+?)\)>', file.read())
  # Retorna transações do último checkpoint, se não tiver retorna array vazio
  return matches[-1].split(',') if matches else [] """

#def dump_log(file, cursor, committed_transactions):

def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value not in lst2]
    return lst3

def find_committed_transactions(file):
  transactions = []

  # Retona pra início do arquivo
  file.seek(0)

  # Percorre arquivo de baixo pra cima
  for line in reversed( list(file) ):
    # Só vai percorrer até encontrar um checkpoint
    #if ("CKPT" in line): break

    matches = re.search('<commit (.+?)>', line)
    # Se encontra commit, adiciona transição na lista
    if matches:
      transactions.append(matches.group(1))

  # Retorna transações em ordem de commit
  return transactions[::-1]


def find_started_transactions(file):
  transactions = []

  # Retona pra início do arquivo
  file.seek(0)

  # Percorre arquivo de baixo pra cima
  for line in reversed( list(file) ):
    # Só vai percorrer até encontrar um checkpoint
    #if ("CKPT" in line): break

    matches = re.search('<start (.+?)>', line)
    # Se encontra commit, adiciona transição na lista
    if matches:
      transactions.append(matches.group(1))

  # Retorna transações em ordem de commit
  return transactions[::-1]



def restore_changes(file, cursor, committed_transactions):
  # Percorre transações commitadas
  for transaction in committed_transactions:
    # Retorna pra início do arquivo
    file.seek(0)

    # Vai para o início da transação
    content = file.read()
    start_transaction = content.index('<start '+ transaction +'>')
    file.seek(start_transaction)

    # Percorre arquivo do start da transição até o final
    for line in list(file):
      # Quando chegar no commit da transição, para
      if ('<commit '+ transaction +'>' in line): break

      matches = re.search('<'+ transaction +',(.+?)>', line)
      # Se for log da transação, atualiza no banco
      if matches:
        # Cria um array com os valores informados no arquivo de log
        values = matches.group(1).split(',')

        #Retorna a coluna da tupla com o ID informado no arquivo
        cursor.execute('SELECT ' + values[1] + ' FROM data WHERE id = ' + values[0])
        tuple = cursor.fetchone()[0]

        # Confere se o valor que esta no arquivo é diferente do valor que está no BD
        if(int(values[3]) != tuple):
          cursor.execute('UPDATE data SET ' + values[1] + ' = ' + values[3] + ' WHERE id = ' + values[0])
          #print_update(transaction, tuple, values)

def undo_changes(file, cursor, committed_transactions, started_transactions):
  uncommitted_transactions = intersection(started_transactions, committed_transactions)
  # Percorre transações commitadas
  for transaction in uncommitted_transactions:
    #print(transaction)
    # Retorna pra início do arquivo
    file.seek(0)

    # Vai para o início da transação
    content = file.read()
    start_transaction = content.index('<start '+ transaction +'>')
    file.seek(start_transaction)

    # Percorre arquivo do start da transição até o final
    for line in reversed(list(file)):

      #original
      #if('<start '+ transaction +'>' in line): break;

      # Quando chegar no commit da transição, para
      #if ('<commit '+ transaction +'>' in line): break

      matches = re.search('<'+ transaction +',(.+?)>', line)
      # Se for log da transação, atualiza no banco
      if matches:
        #print(line)
        # Cria um array com os valores informados no arquivo de log
        values = matches.group(1).split(',')

        #Retorna a coluna da tupla com o ID informado no arquivo
        cursor.execute('SELECT ' + values[1] + ' FROM data WHERE id = ' + values[0])
        #valor da coluna da tupla no disco
        tuple = cursor.fetchone()[0]

        # Confere se o valor da antigo da coluna é diferente do que tem no banco
        #if(int(values[2]) != tuple):
        cursor.execute('UPDATE data SET ' + values[1] + ' = ' + values[2] + ' WHERE id = ' + values[0])
        #print_update(transaction, tuple, values)



def recover(cursor):
  # Abre arquivo da entradaLog apenas para leitura
  file = open('test_files/entradaLog', 'r')

  try:
    # Pega transações startadas
    started_transactions = find_started_transactions(file)

    # Pega transições que foram committadas após o checkpoint
    committed_transactions = find_committed_transactions(file)


    print("REDO:")
    print_redo_transactions(started_transactions, committed_transactions)
    restore_changes(file, cursor, committed_transactions)
    print("UNDO:")
    print_undo_transactions(started_transactions, committed_transactions)
    undo_changes(file, cursor, committed_transactions, started_transactions)

    # Imprime saída
    #mudei isso pra receber commited transaction ao invés de checkpoint ajeitar depois a função
    #print_redo_transactions(started_transactions, committed_transactions)
    #print_undo_transactions(started_transactions, committed_transactions)

    #print_json(cursor)

  finally:
    # Fecha arquivo
    file.close()
