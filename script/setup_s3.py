# Cria a estrutura de pastas (prefixos) do lakehouse no S3.
# Rodar uma única vez:  python setup_s3.py

import boto3

s3 = boto3.client('s3')
BUCKET = 'tech-challenge-3'

PASTAS = [
    'bronze/ano_2023/',
    'bronze/ano_2024/',
    'bronze/ano_2025/',
    'silver/state_2023/',
    'silver/state_2024/',
    'silver/state_2025/',
    'silver/silver_unificado/',
    'gold/',
    'athena-resultados/',
]

for pasta in PASTAS:
    s3.put_object(Bucket=BUCKET, Key=pasta)   # objeto vazio = "pasta" no console
    print(f'✔ s3://{BUCKET}/{pasta}')

print('\n🎉 Estrutura do lakehouse pronta!')