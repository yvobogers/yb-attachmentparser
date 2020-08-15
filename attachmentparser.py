import json
import boto3
import email
import email.policy
import time

s3 = boto3.client('s3')
bucket = '<<src_bucket_name>>'
imgbucket = '<<target_bucket_name>>''

def lambda_handler(event, context):
    '''
    Lambda function that looks for an object in S3 in a given bucket, tries to
    parse it as an email message, looks for any images attached to the email,
    stores these prefixed with a timestamp in a separate bucket, and deletes the
    original object from S3. 
    
    Wrote this for use with a security webcam that only supports email as an 
    external API, so as to collect and process all images captured by the camera
    separately.
    '''

    try:
        oid = event['Records'][0]['s3']['object']['key']
    except Exception as e:
        print('=-=-= Lambda triggered without s3 object key, ignoring =-=-=')
        return

    try:
        s3object = s3.get_object(Bucket=bucket, Key=oid)
        
        # default policy has to be set to parse as email.message.EmailMessage
        msg = email.message_from_bytes(
            s3object['Body'].read(), policy=email.policy.default)
        i = 0
        # check for attachments
        for part in msg.walk():
            i+=1
            ctype = part.get_content_type()
            print(f'=-=-= Message part {i} ctype = {ctype} =-=-=')
            
            # we're only interested in images
            if ctype in ['image/jpeg', 'image/png']:
                filename = part.get_filename()
                currenttime = str(int(round(time.time() * 1000)))
                attachment = part.get_payload(decode=True)
                newfilename = currenttime+'-'+filename
                # dump attachment in image bucket
                s3.put_object(
                    Body=attachment,
                    Bucket=imgbucket,
                    Key=newfilename)
                
                print(f'=-=-= Image stored as: {newfilename} =-=-=')

            else:
                print(f'=-=-= Message part {i} not processed =-=-=')
        
        # delete the source email, only keeping the images
        s3.delete_object(Bucket=bucket,Key=oid)
        
    except Exception as e:
        print(e)
        print(f'Error getting object {oid} from bucket {bucket}')
        raise e

    return 'CONTINUE'
