import boto3
import pandas as pd
import logging
from io import BytesIO
from botocore.exceptions import NoCredentialsError

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def validate_file_not_empty(bucket_name, file_key, s3_client):
    """
    Valida se o arquivo no S3 não está vazio.
    
    Args:
        bucket_name (str): Nome do bucket S3.
        file_key (str): Caminho do arquivo no S3.
        s3_client (boto3.client): Cliente S3.
    
    Returns:
        bool: True se o arquivo não está vazio, False caso contrário.
    """
    try:
        # Baixar o arquivo do S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        
        # Verificar se o DataFrame contém dados
        if df.empty:
            logging.warning(f"O arquivo {file_key} está vazio.")
            return False
        
        logging.info(f"O arquivo {file_key} contém dados.")
        return True
    
    except Exception as e:
        logging.error(f"Erro ao validar o arquivo {file_key}: {e}")
        return False


def process_s3_files(bucket_name, input_prefix, valid_prefix, invalid_prefix, aws_access_key, aws_secret_key):
    """
    Processa arquivos no S3 e valida se não estão vazios.
    
    Args:
        bucket_name (str): Nome do bucket S3.
        input_prefix (str): Prefixo dos arquivos de entrada no S3.
        valid_prefix (str): Prefixo para salvar arquivos válidos no S3.
        invalid_prefix (str): Prefixo para salvar arquivos inválidos no S3.
        aws_access_key (str): Chave de acesso AWS.
        aws_secret_key (str): Chave secreta AWS.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    try:
        # Listar arquivos no prefixo de entrada
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix)
        if 'Contents' not in response:
            logging.warning("Nenhum arquivo encontrado no prefixo de entrada.")
            return
        
        for obj in response['Contents']:
            file_key = obj['Key']
            logging.info(f"Processando arquivo: {file_key}")
            
            # Validar se o arquivo não está vazio
            if validate_file_not_empty(bucket_name, file_key, s3):
                # Mover para o prefixo de arquivos válidos
                valid_key = file_key.replace(input_prefix, valid_prefix)
                s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=valid_key)
                s3.delete_object(Bucket=bucket_name, Key=file_key)
                logging.info(f"Arquivo válido movido para: {valid_key}")
            else:
                # Mover para o prefixo de arquivos inválidos
                invalid_key = file_key.replace(input_prefix, invalid_prefix)
                s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=invalid_key)
                s3.delete_object(Bucket=bucket_name, Key=file_key)
                logging.warning(f"Arquivo inválido movido para: {invalid_key}")
    
    except NoCredentialsError:
        logging.error("Credenciais AWS inválidas.")
    except Exception as e:
        logging.error(f"Erro ao processar arquivos no S3: {e}")