import boto3
import mysql.connector
import csv

# Parámetros de conexión MySQL
db_host = "34.237.90.249"
db_user = "root"
db_password = "utec"
db_name = "profesores_y_cursos"
db_port = 8005
mysql_tables = ["Curso", "Profesor"]

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

    fichero_upload = f"{table_name}"
    with open(fichero_upload, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(resultados)

    cursor.close()
    conexion.close()
    
    # Subir archivo a S3
    s3_client.upload_file(fichero_upload, nombre_bucket, fichero_upload)
    print(f"Ingesta completada y archivo {fichero_upload} subido a S3.")

def main():
    for table in mysql_tables:
        export_mysql_to_csv(table)

if __name__ == "__main__":
    main()
