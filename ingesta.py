import boto3
import mysql.connector
import csv

# Parámetros de conexión MySQL
db_host = "34.237.90.249"
db_user = "root"
db_password = "utec"
db_name = "tienda"
db_port = 8005
db_table = "fabricantes"  # Tabla especificada

# Parámetros para S3
nombreBucket = "stewartmb-output-01"
ficheroUpload = "tienda_mysql.csv"

# Conectar a la base de datos MySQL
conexion = mysql.connector.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_password,
    database=db_name
)

cursor = conexion.cursor()
cursor.execute(f"SELECT * FROM {db_table}")
resultados = cursor.fetchall()

# Escribir los resultados en un archivo CSV
with open(ficheroUpload, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([i[0] for i in cursor.description])  # Escribir el encabezado
    writer.writerows(resultados)  # Escribir los datos

# Cerrar la conexión a la base de datos
cursor.close()
conexion.close()

# Subir el archivo CSV al bucket S3
s3 = boto3.client('s3')
s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)

print("Ingesta completada y archivo subido a S3.")
