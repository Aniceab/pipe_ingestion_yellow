import boto3
import logging
from botocore.exceptions import NoCredentialsError

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def check_file_exists(bucket_name, file_key, aws_access_key, aws_secret_key):
    """
    Verifica se o arquivo existe no bucket S3.

    Args:
        bucket_name (str): Nome do bucket S3.
        file_key (str): Caminho do arquivo no S3.
        aws_access_key (str): Chave de acesso AWS.
        aws_secret_key (str): Chave secreta AWS.

    Returns:
        bool: True se o arquivo existe, False caso contrário.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    try:
        # Tentar obter o objeto no S3
        s3.head_object(Bucket=bucket_name, Key=file_key)
        logging.info(f"O arquivo {file_key} existe no bucket {bucket_name}.")
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.warning(f"O arquivo {file_key} não foi encontrado no bucket {bucket_name}.")
        else:
            logging.error(f"Erro ao verificar o arquivo {file_key}: {e}")
        return False
    except NoCredentialsError:
        logging.error("Credenciais AWS inválidas.")
        return False