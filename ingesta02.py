import boto3
import mysql.connector
import csv
import os

# Parámetros de conexión MySQL
db_host = "34.237.90.249"
db_user = "root"
db_password = "utec"
db_name = "profesores_y_cursos"
db_port = 8005
mysql_tables = ["Profesor", "Curso"]

# Parámetros para S3
nombre_bucket = "proyecto-uni"
s3_client = boto3.client('s3')

def export_mysql_to_csv(table_name):
    conexion = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    cursor = conexion.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    resultados = cursor.fetchall()

    fichero_upload = f"{table_name}.csv"
    with open(fichero_upload, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([i[0] for i in cursor.description])  # Escribir el encabezado
        writer.writerows(resultados)  # Escribir los datos

    cursor.close()
    conexion.close()

    # Definir la ruta de la carpeta en S3 donde se guardará el archivo
    s3_key = f"{table_name}/{fichero_upload}"

    # Subir el archivo CSV a la carpeta correspondiente en el bucket S3
    s3_client.upload_file(fichero_upload, nombre_bucket, s3_key)
    print(f"Ingesta completada y archivo {fichero_upload} subido a S3 en la carpeta {table_name}/.")

    # Eliminar el archivo local después de subirlo a S3 para ahorrar espacio
    os.remove(fichero_upload)

def main():
    for table in mysql_tables:
        export_mysql_to_csv(table)

if __name__ == "__main__":
    main()
