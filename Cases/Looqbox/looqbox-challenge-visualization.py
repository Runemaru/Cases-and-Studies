import mysql.connector
import pandas as pd

try:
    conn = mysql.connector.connect(
        user = '#', 
        password = '#',
        host = '#'
    )
except mysql.connector.Error as err:
    print("Conexão mal-sucedida, erro encontrado: {}".format)
else:
    print(conn.is_connected())

user = conn.cursor()
query ="""
SELECT
    Loja,
    Categoria,
    (SUM(SALES_VALUE) / SUM(SALES_QTY)) AS TM
FROM (
    SELECT
        DATE,
        b.STORE_CODE,
        a.STORE_NAME AS Loja,
        a.BUSINESS_NAME AS Categoria,
        b.SALES_VALUE,
        b.SALES_QTY
    FROM `looqbox-challenge`.`data_store_cad` a 
    LEFT JOIN `looqbox-challenge`.`data_store_sales` b
    ON a.STORE_CODE = b.STORE_CODE
    WHERE b.DATE BETWEEN '2019-01-01' AND '2019-12-31'
    ) AS juntando
    GROUP BY Loja, Categoria
    ORDER BY 1 ASC
"""

user.execute(operation = query)
data = user.fetchall()
columns = [desc[0] for desc in user.description]

df = pd.DataFrame(data = data, columns = columns)
print(df)