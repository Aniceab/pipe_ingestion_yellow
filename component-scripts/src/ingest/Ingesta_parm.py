import boto3
import os
from botocore.exceptions import NoCredentialsError
import requests
import logging
import sys
import os

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def download_files_from_s3(bucket_name, prefix, local_dir):
    """
    Faz o download de arquivos do bucket S3 para um diretório local.
    """
    s3 = boto3.client('s3')
    
    try:
        # Listar objetos no bucket S3 com o prefixo especificado
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in objects:
            for obj in objects['Contents']:
                file_key = obj['Key']
                local_file_path = os.path.join(local_dir, os.path.basename(file_key))
                
                # Fazer o download do arquivo
                s3.download_file(bucket_name, file_key, local_file_path)
                print(f"Arquivo baixado: {local_file_path}")
        else:
            print("Nenhum arquivo encontrado para o prefixo especificado.")
    
    except FileNotFoundError:
        print("Diretório local não encontrado.")
    except NoCredentialsError:
        print("Credenciais inválidas para o AWS.")
    except Exception as e:
        print(f"Erro ao baixar arquivos: {e}")