import os
import yaml
from datetime import datetime
import logging
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../component-scripts/src/ingest')))
from ingesta import upload_url_to_s3
from dq import check_file_exists

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
load_dotenv()


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

    # Configurações do S3
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    bucket_name = config['s3']['input_bucket']


    # Configurações do pipeline
    base_url = config['pipeline']['file_url']
    months = config['pipeline']['months']

    # Processar cada mês
    for month in months:
        # Construir a URL dinamicamente com base no mês
        filename = f"yellow_tripdata_{month}.parquet"
        file_url = f"{base_url}{filename}"  # Concatena a URL base com o nome do arquivo
        logging.info(f"URL do arquivo: {file_url}")

        # Criar o caminho no S3 com partição baseada na data
        s3_key = f"{month}/{filename}"
       
        logging.info(f"Preparando para enviar o arquivo para o S3: s3://{bucket_name}/{s3_key}")

        # Fazer upload diretamente para o S3
    



# Verificar se o arquivo existe no S3
    file_exists = check_file_exists(bucket_name, s3_key, aws_access_key, aws_secret_key)
    if file_exists:
        logging.info(f"Arquivo {s3_key} encontrado no bucket {bucket_name}.")
    else:
        logging.warning(f"Arquivo {s3_key} não encontrado no bucket {bucket_name}.")


if __name__ == "__main__":
    main()