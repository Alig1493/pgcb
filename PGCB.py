import os
from datetime import timedelta

import numpy
import pandas


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

    def processed_files_list(self):
        if 'processed_files.txt' in self.output_files_list():
            with open(f"{self.get_output_filepath()}/processed_files.txt") as file_item:
                processed_files_list = [line.rstrip('\n') for line in file_item]
            return processed_files_list

        print("Processed files list does not exist!")
        print("Making an empty processed files list!")

        with open(f"{self.get_output_filepath()}/processed_files.txt", 'w'):
            pass

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

    column_names_probable = ["Name_of_Power_Station", "Fuel_Type_of_Power_Station", "Powerplant_Under",
                             "Probable_Peak_Day_MW", "Probable_Peak_Evening_MW"]

    column_names_actual = ["Name_of_Power_Station", "Fuel_Type_of_Power_Station", "Powerplant_Under",
                           "Installed_Capacity_MW", "Present_Capacity_MW", "Actual_Peak_Day_MW",
                           "Actual_Peak_Evening_MW",
                           "Gen_Shortfall_Gas_Water_Limitation_MW", "Gen_Shortfall_Machines_Shutdown_MW",
                           "Description_of_Machines_Under_Shutdown"]

    regions = ["Dhaka", "Chittagong", "Comilla", "Mymensingh", "Sylhet", "Khulna", "Barisal", "Rajshahi", "Rangpur"]

    def __init__(self, pgcb_object):
        self.pgcb_object = pgcb_object

    def generate_dataframe(self):
        try:
            if self.pgcb_object.output_files_list():
                return (pandas.read_csv(self.pgcb_object.get_output_filepath()+'actual.csv'),
                        pandas.read_csv(self.pgcb_object.get_output_filepath()+'probable.csv'))
            print("Making empty Dataframes!")
            return pandas.DataFrame(), pandas.DataFrame()
        except FileNotFoundError:
            print("Wrong file or file path")

    def get_all_index_pairs(self, d):
        """Returns a list of tuples. Each tuple holds the start and end indices of the corresponding region's power plants.

        Arguments:
        data -- the raw pandas DataFrame created from source excel file
        d -- a dictionary created from a pandas DataFrame
        """
        dhaka = (self.match_strings(d, 'Name of')[0] + 4, self.match_strings(d, 'Dhaka Area')[0])
        chittagong = (dhaka[1] + 1, self.match_strings(d, 'Chittagong  area')[0])
        comilla = (chittagong[1] + 1, self.match_strings(d, 'Comilla Area')[0])
        mymensingh = (comilla[1] + 1, self.match_strings(d, 'Mymensin')[0])
        sylhet = (mymensingh[1] + 1, self.match_strings(d, 'Sylhet Area')[0])
        khulna = (sylhet[1] + 1, self.match_strings(d, 'Khulna Area')[0])
        barisal = (khulna[1] + 1, self.match_strings(d, 'Barisal Area')[0])
        rajshahi = (barisal[1] + 1, self.match_strings(d, 'Rajshahi Area')[0])
        rangpur = (rajshahi[1] + 1, self.match_strings(d, 'Rangpur Area')[0])

        return [dhaka, chittagong, comilla, mymensingh, sylhet, khulna, barisal, rajshahi, rangpur]

    def generate_output_dataframes(self, actual_dataframe, probable_dataframe):

        """
        if there is a new file in the input file directory,
        then process the file and append the cleaned data to the DataFrame
        column start index; used for resolving the issue of the first column index varying across the raw DataFrames
        """

        for file in self.pgcb_object.input_files_list():
            if file not in self.pgcb_object.processed_files_list():
                filename = self.pgcb_object.get_input_filepath() + str(file)
                df = pandas.read_excel(filename, sheet_name='forecast', header=None).reset_index(drop=True)
                d = df.to_dict()
                date_tuple = self.match_strings(d, 'Date')
                index_list = self.get_all_index_pairs(d)
                csi = self.match_strings(d, 'Name of')[1]

                for i in range(len(index_list)):
                    region = self.regions[i]
                    row_index = index_list[i]
                    df1 = df.iloc[row_index[0]:row_index[1], [csi, csi + 2, csi + 3, csi + 9, csi + 10]].copy()
                    df2 = (df.iloc[row_index[0]:row_index[1],
                           [csi, csi + 2, csi + 3, csi + 5, csi + 6, csi + 7, csi + 8, csi + 11, csi + 12, csi + 13]]
                           .copy())
                    df2['Installed_Minus_Derated_MW'] = pandas.to_numeric(
                        df.iloc[row_index[0]:row_index[1], csi + 5]) - pandas.to_numeric(
                        df.iloc[row_index[0]:row_index[1], csi + 6])
                    df2['Division'] = region
                    df1['Division'] = region
                    df1['Date'] = (df.iloc[date_tuple[0], date_tuple[1] + 1]).strftime('%Y-%m-%d')
                    df2['Date'] = (df.iloc[date_tuple[0], date_tuple[1] + 1] - timedelta(days=1)).strftime('%Y-%m-%d')
                    df1.columns = self.column_names_probable + ['Division'] + ["Date"] + ["File_Name"]
                    df2.columns = self.column_names_actual + ['Installed_Minus_Derated_MW'] + ['Division'] + ['Date']
                    df1 = df1[df1['Name_of_Power_Station'].notnull()]
                    df2 = df2[df2['Name_of_Power_Station'].notnull()]
                    df1 = df1.replace(numpy.nan, "", regex=True)
                    df2 = df2.replace(numpy.nan, "", regex=True)
                    probable_dataframe = pandas.concat([probable_dataframe, df1], axis=0)
                    actual_dataframe = pandas.concat([actual_dataframe, df2], axis=0)

            with open(f"{self.pgcb_object.get_output_filepath()}/processed_files.txt", 'a') as file_content:
                file_content.write(str(file) + "\n")

        self.write_dataframe_to_csv(actual_dataframe, probable_dataframe)

    def write_dataframe_to_csv(self, actual_dataframe, probable_dataframe):
        probable_dataframe.reset_index(drop=True, inplace=True)
        actual_dataframe.reset_index(drop=True, inplace=True)
        actual_dataframe.to_csv(
            self.pgcb_object.get_output_filepath() + "actual.csv",
            columns=["Date"] + self.column_names_actual + ['Installed_Minus_Derated_MW'] + ["Division"],
            index=None)
        probable_dataframe.to_csv(self.pgcb_object.get_output_filepath() + "probable.csv",
                                  columns=["Date"] + self.column_names_probable + ["Division"] + ['File_Name'],
                                  index=None)

    @staticmethod
    def match_strings(d, string):
        """Returns a tuple of row and column indices that identifies the position matching the passed string.

        Arguments:
        d -- a dictionary created from a pandas DataFrame
        string -- the substring to be matched
        """
        for k1, v1 in d.items():
            for k2, v2 in v1.items():
                if type(v2) == str and v2.find(string) != -1:
                    mytuple = (k2, k1)
                    return mytuple
