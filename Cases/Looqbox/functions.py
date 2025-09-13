import mysql.connector
import pandas as pd

def connect_database():
    """Função responsável pela conexão com o banco de dados.
    Da para inserir inputs para inserção de variáveis, contudo manterei de forma manual pro case."""
    try:
        conn = mysql.connector.connect(
            user = '#', 
            password = '#e',
            host = '#'
        )
    except mysql.connector.Error as err:
        print("Conexão mal-sucedida, erro encontrado: {}".format)
    else:
        print(conn.is_connected())
        return conn

def create_query(
        product_code : int = None,
        store_code : int = None,
        date : list[str] = None,
        date_interval : int = None,
        date_order : int = None
            ):
    """Esta função serve para criar uma query de forma mais breve para os times, seus parâmetros são:
    product_code: Recebe um número inteiro e filtra por product_code.
    store_code: Recebe um número inteiro e filtra por store_code.
    date: Recebe uma tupla de strings, e filtra por date, caso vazio traz dados de todo o período.
    date_interval: Recebe um inteiro entre 0 ou 1, sendo 0 sem intervalo e 1 com intervalo de data.
    date_order: Recebe as strings (maior|menor|igual|maior-igual|menor-igual) para fazer consultas de periodos de data maior|menor|igual|maior-igual|menor-igual a tal data. Só funciona se date_interval for igual a 0
    """
    query = "SELECT * FROM `looqbox-challenge`.`data_product_sales` WHERE 1=1"
    params = []

    if product_code is not None:
        query += " AND product_code = %s"
        params.append(product_code)

    if store_code is not None:
        query += " AND store_code = %s"
        params.append(store_code)

    if date is not None and date_interval == 1:
        query += " AND date BETWEEN %s AND %s"
        params.extend(date)

    elif date is not None and date_interval == 0 and date_order.lower() == "menor":
        query += " AND date < %s"
        params.append(date)

    elif date is not None and date_interval == 0 and date_order.lower() == "maior":
        query += " AND date > %s"
        params.append(date)

    elif date is not None and date_interval == 0 and date_order.lower() == "igual":
        query += " AND date = %s"
        params.append(date)

    elif date is not None and date_interval == 0 and date_order.lower() == "maior-igual":
        query += " AND date >= %s"
        params.append(date)

    elif date is not None and date_interval == 0 and date_order.lower() == "menor-igual":
        query += " AND date <= %s"
        params.append(date)
    return query, params

def retrieve_data(query : str, params : list, conn = connect_database()):
    user = conn.cursor()
    data = user.execute(operation = query, params = params)
    data = user.fetchall()

    columns = [desc[0] for desc in user.description]
    user.close()
    return columns, data

def dataframe_constructor(columns, data):
    df = pd.DataFrame(data = data, columns = columns)
    return df