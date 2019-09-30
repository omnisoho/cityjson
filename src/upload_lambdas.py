from src.utils import make_archive
from .aws_access_manager import AccessManager

def main():
    make_archive('./src/lambdas/lambdas', 'zip', './src/lambdas/')

    s3_client = AccessManager().s3_client_instance()
    s3_client.upload_file(Filename ='./src/lambdas/lambdas.zip', Bucket='prj-ss-lambdas', Key='src_zip/lambdas.zip')

if __name__ == '__main__':
    main()
