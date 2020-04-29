# config: utf8
import boto3
import pandas as pd


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






def abre_file_como_df(bucket_name, file_to_read):
     "Esta funcion abre un archivo del bucket en un dataframe"

     ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
     client = boto3.client('s3', region_name='us-east-1')

     #create a file object using the bucket and object key. 
     fileobj = client.get_object(Bucket=bucket_name, Key=file_to_read) 

     df = pd.read_csv(fileobj['Body'], sep="\t")

     return df



def upload_pickle_s3(bucket, pkl_file):

      key=pkl_file

      ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
      s3_resource = boto3.resource('s3')
      
      s3_resource.Object(bucket,key)





def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file

    ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
    s3_client = boto3.client('s3', region_name='us-east-1')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
