import Cases.Looqbox.functions as udf
import pandas as pd

query, params = udf.create_query(product_code = 18,
                                 store_code = 1,
                                 date = "2019-01-01",
                                 date_interval = 0,
                                 date_order = "igual")

columns, data = udf.retrieve_data(query = query, params = params)
df = udf.dataframe_constructor(columns = columns, data = data)
x = df.head(5)
print(x)