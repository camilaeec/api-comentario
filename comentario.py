import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json) - MANTENIENDO LOS PRINTS ORIGINALES
    print("=== EVENTO RECIBIDO ===")
    print(event)
    
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    
    # OBTENER NOMBRE DEL BUCKET S3 DESDE VARIABLES DE ENTORNO
    nombre_bucket = os.environ["BUCKET_NAME"]
    print(f"Tabla DynamoDB: {nombre_tabla}")
    print(f"Bucket S3: {nombre_bucket}")
    
    # Proceso - GENERAR UUID V1 (MANTENIENDO LÓGICA ORIGINAL)
    uuidv1 = str(uuid.uuid1())
    print(f"UUID V1 generado: {uuidv1}")
    
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        },
        'timestamp_creacion': datetime.now().isoformat()
    }
    
    # 1. GUARDAR EN DYNAMODB (MANTENIENDO LÓGICA ORIGINAL)
    print("=== GUARDANDO EN DYNAMODB ===")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response_dynamo = table.put_item(Item=comentario)
    print("✅ Guardado en DynamoDB exitoso")
    print(f"Response DynamoDB: {response_dynamo}")
    
    # 2. NUEVA FUNCIONALIDAD: GUARDAR EN S3 (INGESTA PUSH)
    print("=== INICIANDO INGESTA PUSH A S3 ===")
    try:
        s3_client = boto3.client('s3')
        
        # Crear estructura de carpetas organizada
        timestamp = datetime.now().strftime("%Y/%m/%d/%H-%M-%S")
        s3_key = f"comentarios/tenant_{tenant_id}/{timestamp}_{uuidv1}.json"
        
        # Preparar datos para S3 (incluir metadata adicional)
        comentario_s3 = comentario.copy()
        comentario_s3['_metadata'] = {
            'procesado_por': 'lambda-api-comentario',
            'fecha_procesamiento': datetime.now().isoformat(),
            'version': '1.0',
            'stage': os.environ.get('STAGE', 'desconocido')
        }
        
        comentario_json = json.dumps(comentario_s3, indent=2)
        
        # Subir a S3 (Estrategia Push)
        response_s3 = s3_client.put_object(
            Bucket=nombre_bucket,
            Key=s3_key,
            Body=comentario_json,
            ContentType='application/json',
            Metadata={
                'tenant-id': tenant_id,
                'uuid': uuidv1,
                'procesado-en': datetime.now().isoformat()
            }
        )
        
        print("✅ Guardado en S3 exitoso")
        print(f"📁 Archivo S3: s3://{nombre_bucket}/{s3_key}")
        print(f"Response S3: {response_s3}")
        
    except Exception as e:
        print(f"❌ Error al guardar en S3: {str(e)}")
        # No falla la función completa si S3 falla, solo registra el error
    
    # Salida (json) - MANTENIENDO ESTRUCTURA ORIGINAL MEJORADA
    print("=== PREPARANDO RESPUESTA ===")
    print(comentario)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'mensaje': 'Comentario procesado exitosamente',
            'comentario': comentario,
            'almacenamiento': {
                'dynamodb': {
                    'tabla': nombre_tabla,
                    'estado': 'exitoso'
                },
                's3': {
                    'bucket': nombre_bucket,
                    'ruta_archivo': f"s3://{nombre_bucket}/{s3_key}" if 's3_key' in locals() else 'no_disponible',
                    'estado': 'exitoso' if 'response_s3' in locals() else 'fallido'
                }
            },
            'metadata': {
                'uuid_generado': uuidv1,
                'timestamp': datetime.now().isoformat()
            }
        })
    }
