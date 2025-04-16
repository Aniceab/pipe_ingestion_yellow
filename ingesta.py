import requests
import boto3
from botocore.exceptions import NoCredentialsError

def download_file(url, local_path):
    """
    Faz o download de um arquivo de uma URL e salva localmente.
    """
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, 'wb') as file:
            file.write(response.content)
        print(f"Arquivo baixado com sucesso: {local_path}")
    else:
        raise Exception(f"Falha ao baixar o arquivo. Status code: {response.status_code}")

def upload_to_s3(local_path, bucket_name, s3_key, aws_access_key, aws_secret_key):
    """
    Faz o upload de um arquivo local para um bucket S3.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    try:
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"Arquivo enviado para o S3 com sucesso: s3://{bucket_name}/{s3_key}")
    except FileNotFoundError:
        print("Arquivo não encontrado.")
    except NoCredentialsError:
        print("Credenciais inválidas para o AWS.")

# Exemplo de uso
if __name__ == "__main__":
    # URL do arquivo para download
    file_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    local_file_path = "yellow_tripdata_sample_2023-01.parquet"

    # Configurações do S3
    bucket_name = "yellow-taxi-files-anice"
    s3_key = "ingesta_file/yellow_tripdata_sample_2023-01.parquet"
    aws_access_key = "AKIAQ62SYYVDQTYHY7WP"
    aws_secret_key = "IivboAdebexkiXsW+6BJHfPWb3WmYYUJKiLxTOHW"

    try:
        # Baixar o arquivo  
        download_file(file_url, local_file_path)

        # Fazer upload para o S3
        upload_to_s3(local_file_path, bucket_name, s3_key, aws_access_key, aws_secret_key)
    except Exception as e:
        print(f"Erro: {e}")