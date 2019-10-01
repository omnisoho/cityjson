from src.utils import make_archive
from .aws_access_manager import AccessManager

def main():

    output_filepath = './zip_files/lambdas'
    archive_format = 'zip'
    dir_to_zip = './src/lambdas/'
    
    make_archive(output_filepath, archive_format, dir_to_zip)

    s3_client = AccessManager().s3_client_instance()
    s3_client.upload_file(Filename =output_filepath + '.' + archive_format, Bucket='gisgeometry.raw.data', Key='lambdas/lambdas.zip')

if __name__ == '__main__':
    main()
