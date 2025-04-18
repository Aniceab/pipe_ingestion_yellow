import os
import yaml
from datetime import datetime
import sys
from ingest.ingesta import upload_url_to_s3
# Adicionar o caminho do diretório onde cred.py está localizado
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from cred import get_secret
import logging
import boto3
from dotenv import load_dotenv
# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
load_dotenv()

    
# Configuração do cliente S3


def load_config(config_path):
    """
    Carrega as configurações do arquivo config.yaml.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
    


def main():
    # Caminho para o arquivo de configuração
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
  
    
    # Carregar configurações
    config = load_config(config_path)

    


    # Recupera as credenciais do Secrets Manager
    #aws_access_key,aws_secret_key= get_secret('s3-access-creds')
    # Configurações do S3
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    bucket_name = config['s3']['input_bucket']

    
    # Configurações do pipeline
    base_url = config['pipeline']['file_url']
    months = config['pipeline']['months']
    
    # Obter a data de processamento
    current_date = datetime.now()
    processing_date = '2025-01'
    
    
    
    # Processar cada mês
    for month in months:
        # Construir a URL dinamicamente com base no mês
        filename = f"yellow_tripdata_{month}.parquet"
        #file_url=base_url
        file_url = f"{base_url}{filename}"  # Concatena a URL base com o nome do arquivo
        print(f"URL do arquivo: {file_url}")
        # Criar o caminho no S3 com partição baseada na data
        s3_key = f"{month}/{filename}"
        
        # Log para indicar que o arquivo será sobrescrito, se já existir
        logging.info(f"Preparando para enviar o arquivo para o S3: s3://{bucket_name}/{s3_key}")
        
        # Fazer upload diretamente para o S3
        upload_url_to_s3(file_url, bucket_name, s3_key, aws_access_key, aws_secret_key)


if __name__ == "__main__":
    main()