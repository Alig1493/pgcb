import os
import pandas
import traceback


class PgcbFiles(object):
    
    def __init__(self, input_file_dir='/PGCBFiles/', output_file_dir='/PGCBOut/'):
        self.input_path = os.getcwd() + input_file_dir
        self.output_path = os.getcwd() + output_file_dir

    def set_input_filepath(self, input_file_dir):
        self.input_path = os.getcwd() + input_file_dir

    def set_output_filepath(self, output_file_dir):
        self.output_path = os.getcwd() + output_file_dir

    def get_input_filepath(self):
        return self.input_path

    def get_output_filepath(self):
        return self.output_path

    def input_files_list(self):
        files_list = self.directory_empty(self.get_input_filepath())
        if not files_list:
            return []
        return files_list

    def output_files_list(self):
        files_list = self.directory_empty(self.get_output_filepath())
        if not files_list:
            return []
        return files_list

    def generate_dataframe(self):
        try:
            if self.output_files_list():
                return (pandas.read_csv(self.get_output_filepath()+'actual.csv'),
                        pandas.read_csv(self.get_output_filepath()+'probable.csv'))
            print("Making empty Dataframes!")
            return pandas.DataFrame(), pandas.DataFrame()
        except FileNotFoundError:
            print("Wrong file or file path")

    def _processed_files_list(self):
        if 'processed_files.txt' in self.output_files_list():
            with open('processed_files.txt') as file_item:
                processed_files_list = [line.rstrip('\n') for line in file_item]
            return processed_files_list
        print("Processed files list does not exist!")
        return []

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

    def __init__(self):
        pass
