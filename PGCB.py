import os
import pandas

from exception import EmptyDirectoryException


class PgcbFiles(object):
    
    def __init__(self, input_file_dir='/PGCBFiles/', output_file_dir='/PGCBOut/'):
        self.input_path = os.getcwd() + input_file_dir
        self.output_path = os.getcwd() + output_file_dir

    def get_input_filepath(self):
        return self.input_path

    def get_output_filepath(self):
        return self.output_path

    def list_of_input_files(self):
        files_list = self.directory_empty(self.input_path)
        try:
            if not files_list:
                raise EmptyDirectoryException("Directory is empty!")
        except EmptyDirectoryException as e:
            print(e)
        return files_list

    def list_of_output_files(self):
        files_list = self.directory_empty(self.output_path)
        try:
            if not files_list:
                raise EmptyDirectoryException("Directory is empty!")
        except EmptyDirectoryException as e:
            print(e)
        return files_list

    @staticmethod
    def directory_empty(filepath):
        try:
            return [file_item for file_item in os.listdir(filepath) if not file_item.startswith('.')]
        except OSError as e:
            print(e)

    @staticmethod
    def file_exists(path='', filename=''):
        return os.path.isfile(os.path.join(path, filename))


class PandasProcess(object):

    def __init__(self, filename, sheet_name):
        pass
