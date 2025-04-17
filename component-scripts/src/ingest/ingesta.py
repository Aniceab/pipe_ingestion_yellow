import requests
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

def upload_url_to_s3(url, bucket_name, s3_key, aws_access_key, aws_secret_key):
    """
    Faz o download de um arquivo de uma URL e envia diretamente para o S3 sem salvar localmente.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        s3.upload_fileobj(response.raw, bucket_name, s3_key)
        print(f"Arquivo enviado diretamente para o S3: s3://{bucket_name}/{s3_key}")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar o arquivo da URL: {e}")
    except NoCredentialsError:
        print("Credenciais AWS inválidas.")
    except Exception as e:
        print(f"Erro inesperado: {e}")
