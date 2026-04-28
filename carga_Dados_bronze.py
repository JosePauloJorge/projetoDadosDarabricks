#################################################################
#              
# Autor: José Paulo
# Data: 2021-08-24
# Comentarios: Processo para carregar os dados para a bronze
##################################################################

  
#################################################################
#            Criando os Impports
#################################################################
from pyspark.sql.functions import current_timestamp

#################################################################
# Criando as variaveis dos paths para uso na aplicação   
#################################################################
resource_path = '/Volumes/bikestore/resource/01-origem/'
bronze_path   = "bikestore.bronze"
                
#################################################################
# Criando o mapeamento dos dados da camada Landing 
#################################################################                
bronze_map = {
        "tmp_bronze_brands":      f"{resource_path}brands.csv",
        "tmp_bronze_categories":  f"{resource_path}categories.csv/",
        "tmp_bronze_customers":   f"{resource_path}customers.csv/",
        "tmp_bronze_order_items": f"{resource_path}order_items.csv/",
        "tmp_bronze_orders":      f"{resource_path}orders.csv/",
        "tmp_bronze_products":    f"{resource_path}products.csv/",
        "tmp_bronze_staffs":      f"{resource_path}staffs.csv/",
        "tmp_bronze_stocks":      f"{resource_path}stocks.csv/",
        "tmp_bronze_stores":      f"{resource_path}stores.csv/",
}
#################################################################
# Lendo cada arquivo da Landing Zone e gravando na camada bronze
#################################################################
for name, path in bronze_map.items():
    #################################################################
    # Lendo cada arquivo da Landing Zone e gerando o Dataframe
    #################################################################
    try:    
        df = ( 
            spark.read.csv(path, header=True, inferSchema=True)
                .withColumn("_ingestion_timestamp", current_timestamp())
        )
    except Exception as e:
        #######################################################
        # Falta colocaro processo de gravação de log de Erro
        #######################################################
        print(f"Error: {e}")
        dbutils.notebook.exit(f"Error: {e}")
    finally:
            ##################################################################################################
            # Falta colocaro processo de gravação de log de Processamento finalizado com sucesso    
            ##################################################################################################
            print(f"Sucesso ao gravar o arquivo {name} na camada Bronze")    

    ########################################################################################
    # Gravando o DF de cada arquivoda landing Zone para dentro da Camada Bronze.
    ########################################################################################
    try:
        (
            df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true") \
                    .saveAsTable(f"{bronze_path}.{name.replace("tmp_bronze_", "")}")
        )
    except Exception as e:
        #######################################################
        # Falta colocaro processo de gravação de log de Erro
        #######################################################
        print(f"Error: {e}")
        dbutils.notebook.exit(f"Error: {e}")
    finally:
            ##################################################################################################
            # Falta colocaro processo de gravação de log de Processamento finalizado com sucesso    
            ##################################################################################################
            print(f"Sucesso ao gravar o arquivo {name} na camada Bronze")

