from PGCB import PgcbFiles, PandasProcess


def main():
    pgcb_object = PgcbFiles()
    pgcb_object.output_files_list()
    pandas_object = PandasProcess(pgcb_object)
    df1, df2 = pandas_object.generate_dataframe()
    print(df1.head())


if __name__ == '__main__':
    main()
