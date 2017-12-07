from PGCB import PgcbFiles


def main():
    pgcb_object = PgcbFiles()
    pgcb_object.output_files_list()
    df1, df2 = pgcb_object.generate_dataframe()
    print(df1.head())


if __name__ == '__main__':
    main()
