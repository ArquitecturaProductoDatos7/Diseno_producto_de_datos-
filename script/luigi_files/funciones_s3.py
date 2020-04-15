# config: utf8
import boto3


def create_s3_bucket(bucket_name):
    """
    Funcion que crea el bucket en AWS. 
    El busket no tiene acceso publico y esta encriptado.
    Input:
        bucket_name: Nombre del bucket
    """
    s3_client = boto3.resource("s3")
    
    exito = 0 # Bandera para ver si se creo exitosamente
    try:
        response = s3_client.create_bucket(Bucket=bucket_name)
        print("***** Successfully create S3 bucket *****\n")
        exito = 1
        
    except Exception as error:
        print ("***** Couldn't create S3 bucket *****\n", error)
    
    return exito



def s3_bloquear_acceso_publico(bucket_name):
    """ 
    Funcion que bloquea todo el acceso publico del bucket
    """
    client = boto3.client('s3')
        
    response = client.put_public_access_block(
            Bucket= bucket_name,
            #ContentMD5='string',
            PublicAccessBlockConfiguration={
                        'BlockPublicAcls': True,
                        'IgnorePublicAcls': True,
                        'BlockPublicPolicy': True,
                        'RestrictPublicBuckets': True
                    }
        )
    
    
    
    
def s3_encriptado(bucket_name):
    """ 
    Funcion que canbia los setting del bucket para que sea encriptado
    """
    client = boto3.client('s3')
    
    response = client.put_bucket_encryption(
            Bucket = bucket_name,
            #ContentMD5='string',
            ServerSideEncryptionConfiguration={
                        'Rules': [{
                            'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256',
                                                                   # 'KMSMasterKeyID': 'string'
                                                                  }
                                  },
                                 ]
                }
            )
