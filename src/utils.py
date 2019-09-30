import re
import shutil

def replace_special_chars(preformatted, replacement_char):
    return re.sub('[^a-zA-Z0-9\n\.]', replacement_char, preformatted)

def make_archive(out_filename, archive_format, dir_to_archive):
    shutil.make_archive(out_filename, archive_format, dir_to_archive)