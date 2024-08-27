from handling import standarise_names, read_spreadsheets

if __name__ == '__main__':
    new_df = standarise_names(read_spreadsheets(
        'C:/Users/IamNo/Desktop/DataFiles/FilesIn/census/2021 Census GCP All Geographies for AUS/AUS/'
        '2021Census_G01_AUS_AUS.csv', 'csv'),
        read_spreadsheets('C:/Users/IamNo/Desktop/DataFiles/FilesIn/census/Metadata/Metadata_2021_GCP_DataPack_R1_R2'
                          '.xlsx', 'xlsx', 0)["Metadata_2021_GCP_DataPack_R1_R2.xlsx"]['Cell Descriptors Information'],
        "DataPackfile", "Short", "Long"
    )
    print(new_df["2021Census_G01_AUS_AUS.csv"].collect().collect_schema().names())
