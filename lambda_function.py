import boto3
import json
import os
import subprocess
from tempfile import TemporaryDirectory
from io import BytesIO
from urllib.parse import unquote_plus

print("Contents of /opt/bin directory:")
print(os.listdir('/opt/bin'))


def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))
    
    # Add /opt to PATH
    os.environ['PATH'] = f"{os.environ['PATH']}:/opt"
    
    s3 = boto3.client('s3')

    # Input and output bucket names
    input_bucket = os.environ['INPUT_BUCKET']
    output_bucket = os.environ['OUTPUT_BUCKET']

    # Get the input video file from S3
    encoded_key = event['Records'][0]['s3']['object']['key']
    key = unquote_plus(encoded_key)
    print(f"Attempting to get S3 object with key: {key}") # Add this line to log the S3 object key
    with TemporaryDirectory() as tempdir:
        output_file = os.path.join(tempdir, 'output_audio.mp3')

        # Download the video file from S3 as streaming bytes
        response = s3.get_object(Bucket=input_bucket, Key=key)
        input_stream = response['Body']

        # Extract audio from video using FFmpeg
        # Extract audio from video using FFmpeg
        command = f'/opt/bin/ffmpeg -i pipe:0 -vn -acodec mp3 -f mp3 pipe:1'


        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        output_data, stderr_data = process.communicate(input=input_stream.read())

        # If an error occurs during the FFmpeg execution, log the error message
        if process.returncode != 0:
            print(f"FFmpeg error: {stderr_data.decode('utf-8')}")
        else:
            print(f"FFmpeg output: {stderr_data.decode('utf-8')}")

        # Upload the extracted audio to S3
        output_key = f'{os.path.splitext(key)[0]}.mp3'
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=BytesIO(output_data))

    return {
        'statusCode': 200,
        'body': 'Audio extracted and uploaded to S3'
    }
