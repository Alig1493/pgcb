import os
import pandas


class PgcbFiles(object):
    
    def __init__(self, actual_filename='actual.csv', probable_filename='probable.csv',
                 input_file_dir='/PGCBFiles/', output_file_dir='/PGCBOut/'):
        self.input_path = os.getcwd() + input_file_dir
        self.output_path = os.getcwd() + output_file_dir
        self.actual_file = self.output_path + actual_filename
        self.probable_file = self.output_path + probable_filename

    def list_of_input_files(self):
        empty, files_list = self.input_directory_empty()
        if empty:
            raise Exception("Input directory is empty!")
        return files_list

    def list_of_processed_files(self):
        empty, files_list = self.output_directory_empty()
        if empty:
            raise Exception("Input directory is empty!")
        return files_list

    def input_directory_empty(self):
        files_list = [file_item for file_item in os.listdir(self.input_path) if not file_item.startswith('.')]
        if not files_list:
            return True, []
        return False, files_list

    def output_directory_empty(self):
        files_list = [file_item for file_item in os.listdir(self.output_path) if not file_item.startswith('.')]
        if not files_list:
            return True, []
        return False, files_list

    @staticmethod
    def file_exists(path='', filename=''):
        return os.path.isfile(os.path.join(path, filename))


class PandasProcess(object):

    def __init__(self, filename, sheet_name):
        pass
