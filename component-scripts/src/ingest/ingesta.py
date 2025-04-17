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

# Exemplo de uso
# if __name__ == "__main__":
#     # URL do arquivo para download
#     file_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
#     local_file_path = "yellow_tripdata_sample_2023-01.parquet"

#     # Configurações do S3
#     bucket_name = "yellow-taxi-files-anice"
#     s3_key = "ingesta_file/yellow_tripdata_sample_2023-01.parquet"
#     aws_access_key = "AKIAQ62SYYVDQTYHY7WP"
#     aws_secret_key = "IivboAdebexkiXsW+6BJHfPWb3WmYYUJKiLxTOHW"

#     upload_url_to_s3(file_url, bucket_name, s3_key, aws_access_key, aws_secret_key)
