import apache_beam as beam 
import pandas as pd 
import datetime as datetime
from apache_beam.options.pipeline_options import PipelineOptions
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import json

#connection code

server="sqlserver-sathish.westus2.cloudapp.azure.com"
database="52346923_Sagar_DB"
username="52346953_Sagar"
password=quote_plus("Pluto@123")
driver=quote_plus("ODBC Driver 17 for SQL Server")

connection_string=(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    f"?driver={driver}"
)

#ingestion code
class writetosql(beam.DoFn):

    def __init__(self,table_name):
        self.table_name=table_name

    def setup(self):
        self.engine=create_engine(connection_string)

    def process(self,element):
        df=pd.DataFrame([element])
        df.to_sql(self.table_name,self.engine,if_exists='append',index=False,schema='dbo')
        yield f"wrote it to the table {self.table_name}"

def safe_number(value):
    if value=="":
        return None
    else:
        return int(value)

def safe_number_float(value):
    if value=="":
        return None
    else:
        return round(float(value),2)


#sale_id,product_id,customer_id,sale_date,quantity_sold,unit_price
def parse_sales(element):
    columns=["sale_id","product_id","customer_id","sale_date","quantity_sold","unit_price"]
    field=element.split(",")

    y=len(columns)-len(field)
    while(y):
        field.append("")

    return {
        "sale_id":field[0].strip(),
        "product_id":field[1].strip(),
        "customer_id":field[2].strip(),
        "sale_date":field[3].strip(),
        "quantity_sold":safe_number(field[4]),
        "unit_price": safe_number_float(field[5])
    }

def normalize_date(element):
    dt=element.get("sale_date","")

    if dt=="":
        element['sale_date']=None
        return element
    format="%d/%m/%Y"
    try:
        ndt=datetime.datetime.strptime(dt,format)
        ndt=ndt.strftime("%Y-%m-%d")
        element['sale_date']=ndt
        return element
    except ValueError:
        element['sale_date']=None
    
    return element

def dict_to_str(element):
    return json.dumps(element)

def str_to_dict(element):
    return json.loads(element)

def run():
    options=PipelineOptions()
    with beam.Pipeline(options=options) as p:
        sales=(
            p
            |"reading the sales data">> beam.io.ReadFromText("gs://dna-poc-training_sagar/sales_data(in).csv",skip_header_lines=True)
            |"parsing the data">> beam.Map(parse_sales)
            |"normalizing dates">> beam.Map(normalize_date)
            |"fitering not null and null">> beam.Filter( lambda  row: all(v is not None for v in row.values()))
            |"converting disting rows to str">> beam.Map(dict_to_str)
            |"fileting the disticnt rows">> beam.Distinct()
            |"converting str to dict">> beam.Map(str_to_dict)
            |"printing the sales">> beam.ParDo(writetosql("fact_sales"))
    )

if __name__=='__main__':
    run()
