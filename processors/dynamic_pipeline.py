from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

class DynamicPipeline:
    """
    Pipeline dinâmico para processar dados Bronze -> Silver com validação.
    Compatível com Serverless Compute.
    """
    
    def __init__(self, spark, bronze_map, validation_map, silver_path, quarantine_path):
        self.spark = spark
        self.bronze_map = bronze_map
        self.validation_map = validation_map
        self.silver_path = silver_path
        self.quarantine_path = quarantine_path
        
    def _extract_table_name(self, temp_table_name):
        """Extrai o nome real da tabela removendo prefixo tmp_bronze_"""
        return temp_table_name.replace("tmp_bronze_", "")
    
    def _read_csv(self, file_path):
        """Lê arquivo CSV com opções padrão"""
        return self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
    
    def _validate_and_split(self, df, required_columns):
        """
        Valida e separa dados válidos/inválidos.
        Retorna tupla (df_valid, df_invalid)
        """
        # Constrói condição de validação
        validation_condition = None
        for col_name in required_columns:
            if col_name in df.columns:
                condition = col(col_name).isNotNull()
                validation_condition = condition if validation_condition is None else validation_condition & condition
        
        if validation_condition is None:
            # Sem validação, todos são válidos
            return df, None
        
        # Separa válidos e inválidos
        df_valid = df.filter(validation_condition)
        df_invalid = df.filter(~validation_condition)
        
        return df_valid, df_invalid
    
    def _process_table(self, temp_table_name, file_path):
        """Processa uma tabela: lê, valida e grava"""
        table_name = self._extract_table_name(temp_table_name)
        
        print(f"\n{'='*60}")
        print(f"Processando: {table_name}")
        print(f"Origem: {file_path}")
        print(f"{'='*60}")
        
        # 1. Leitura
        df = self._read_csv(file_path)
        
        # 2. Validação
        required_columns = self.validation_map.get(table_name, [])
        
        if required_columns:
            print(f"✅ Validando colunas: {', '.join(required_columns)}")
            df_valid, df_invalid = self._validate_and_split(df, required_columns)
        else:
            print("⚠️  Sem regras de validação - todos os registros são válidos")
            df_valid = df
            df_invalid = None
        
        # 3. Gravação na Silver
        pipeline_run = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        df_valid_with_metadata = df_valid \
            .withColumn("_processed_at", current_timestamp()) \
            .withColumn("_pipeline_run", lit(pipeline_run))
        
        silver_table = f"{self.silver_path}.{table_name}"
        df_valid_with_metadata.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(silver_table)
        
        print(f"✓ Silver: Dados gravados em {silver_table}")
        
        # 4. Gravação na Quarentena (se houver inválidos)
        if df_invalid is not None:
            df_invalid_with_metadata = df_invalid \
                .withColumn("_quarantined_at", current_timestamp()) \
                .withColumn("_pipeline_run", lit(pipeline_run)) \
                .withColumn("_reason", lit("Colunas obrigatórias com valores nulos"))
            
            quarantine_table = f"{self.quarantine_path}.{table_name}"
            df_invalid_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(quarantine_table)
            
            print(f"⚠ Quarentena: Registros inválidos gravados em {quarantine_table}")
        
        print(f"✓ Tabela {table_name} processada com sucesso")
    
    def run(self):
        """Executa o pipeline completo"""
        print("\n" + "="*60)
        print("🚀 INICIANDO PIPELINE BRONZE → SILVER")
        print("="*60)
        print(f"Total de tabelas: {len(self.bronze_map)}")
        
        # Processa cada tabela
        for temp_table_name, file_path in self.bronze_map.items():
            try:
                self._process_table(temp_table_name, file_path)
            except Exception as e:
                print(f"❌ Erro ao processar {temp_table_name}: {str(e)}")
                raise
        
        print("\n" + "="*60)
        print("🏁 PIPELINE FINALIZADO COM SUCESSO")
        print("="*60)
