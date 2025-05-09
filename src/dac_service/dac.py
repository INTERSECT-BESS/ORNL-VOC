import argparse

import pandas as pd

from datetime import datetime

def data_reduction(df, metadata, cutoff):
    '''
    Perform the data reduction for the given LICOR data.
    
    Args:
        data_file String path to the xlsx format data file
        sheet_name String sheet name where data_file has the data saved
        meradata_file String path to the xlsx format metadata file.
        cutoff Float for the cutoff point for Standard Deviation over all plants for a gas to be included in the high 
            variance list
    Returns:
        A Dictionary of strings in the format:
        {
            "data": "csv data for output file in string format",
            "abnormal": [ "list of plant tags with abnormal metadata" ],
            "above_threshold": {
                "plant1: {
                    [ "list of gases for plant1 with concentrations above the threshold of 0.00005 and 3 * blank std" ]
                }
            },
            high_variance": [ "list of gases with a standard deviation over plants of at least the cutoff argument " ]
        }
    '''
    
    # DO1 is 0 or 1, with 1 represent one of the middle three measurements which are to be kept, so throw away anything with 0
    df = df[:][df.DO1 == 1]
    
    # Convert all time strings into hour + minute datetimes
    df["time_string"] = df["time_string"].apply(lambda x: x.to_pydatetime().time())
    
    
    # Convert all time strings into hour + minute datetimes
    metadata["PTR Start Time"] = metadata["PTR Start Time"].apply(lambda x: x.to_pydatetime().time())   
    #metadata["PTR Start Time"].apply(lambda x: x.dt.time)
    
    # Divide the results of the two LICORs.
    licor_names = metadata.LICOR.unique()
    
    # Dictionary from LICOR names to metadata for that LICOR's measurements
    metadata_per_licor = {}
    
    # Dictionary from LICOR names to the data for that LICOR's blanks
    blanks_per_licor = {}
    
    # Dictionary from LICOR names to three times the Standard Deviation for the measurements on that LICOR's blank or the 
    # floor value of 5 x 10 ^ -12
    blank_std_per_licor = {}
    
    # Dictionary of dictionaries, where the first key is the LICOR name, the second key is the species name, and the value is
    # the Dataframe containing that species's measurements.
    df_per_licor = {}
    
    # Organize the data for each LICOR's measurements in turn.
    for licor in licor_names:   
        
        # Grab the metadata for this LICOR's measurements
        metadata_per_licor[licor] = metadata[:][metadata.LICOR == licor]  
        
        # Get only the rows with blank data in them.
        blank_metadata = \
            metadata_per_licor[licor][metadata_per_licor[licor]["Plant Tag"].str.startswith('Blank')]
            
        blanks = []
        
        for index, md_row in metadata_per_licor[licor].iterrows():
            
            if md_row["Plant Tag"].startswith("Blank"):
            
                # The first blank will exist be the first measurement. Get its start time.
                start = md_row["PTR Start Time"]
                
                # Get the first blank's position in the list of all measurements
                metadata_index = metadata[metadata["PTR Start Time"] == start].index[0]
                #end1 = metadata_per_licor[licor].iloc[1]["PTR Start Time"]
                
                
                if metadata_index + 1 == metadata.shape[0]:
                    
                    # If this was the last measurement of all, read from the start to the end of the file
                    blanks.append(df[df["time_string"] >= start])
                else:
                    
                    # If this was not the last measurement for all LICORS, get the next measurement's start time and the data between
                    # those two times
                    end = metadata.iloc[metadata_index + 1]["PTR Start Time"]
                    blanks.append(df[(df["time_string"] >= start) & (df["time_string"] < end)])
                
        
        # Combine the first and last blank measurements
        blank = pd.concat(blanks)
        
        # Remove all the metadata columns
        blank = remove_non_gas_columns(blank)
    
        # Make a copy of the blank's data and take the average of each gas's concentration
        blank_mean = blank.copy()
        blank_mean[blank_mean.columns.difference(['time_string', 'time_number'])] = \
            blank_mean[blank_mean.columns.difference(['time_string', 'time_number'])].mean()
        blanks_per_licor[licor] = blank.mean(axis=0)
        
        # Create a copy of the blank data and take the standard deviation
        blank_std = blank.copy().std()
        blank_std = blank_std.mul(3)
        
        # This is the floor value for the gas concentration magnitude check. Round up all std values to it if they are lower.
        blank_std[blank_std < 0.000000000005] = 0.000000000005
        
        blank_std_per_licor[licor] = blank_std
        
        # The index for the current measurement for this LICOR. Start at 1 to skip the initial blank
        i = 1
        df_per_licor[licor] = {}
        
        # Iterate through all metadata rows for measurments, stopping before the last measurement which will be the second
        # blank
        while i < metadata_per_licor[licor].shape[0]:
            
            # Skip rows for the blanks
            if metadata_per_licor[licor].iloc[i]["Plant Tag"].startswith("Blank"):
                i += 1
                continue
            
            # Get the start time for the current measurement
            start = metadata_per_licor[licor].iloc[i]["PTR Start Time"]
            
            # Get the measurement's position in the list of all measurements
            metadata_index = metadata[metadata["PTR Start Time"] == start].index[0]
            #end = metadata_per_licor[licor].iloc[i + 1]["PTR Start Time"]
            
            # Read this measurement's species identifier
            name = metadata_per_licor[licor].iloc[i]["Plant Tag"]
            
            if i < metadata_per_licor[licor].shape[0] - 1:
                
                # The measurement ends with the start of the next one
                end = metadata.iloc[metadata_index + 1]["PTR Start Time"]
                
                # Get the data between this current measurement's start and end times.
                df_per_licor[licor][name] = df[(df["time_string"] >= start) & (df["time_string"] < end)]
                
            else:
                
                # This is the last metadata row, so get all data after the start
                df_per_licor[licor][name] = df[df["time_string"] >= start]
                
            i += 1
     
    # List of all abnormal tags, defined as any tag wherein at least one timestamp had a 21 m/z or PC_Pressure value more than
    # 20% away from the expected values of 2200 or 400 respectively
    abnormal_tags = []
    
    # Find all abnormal tags
    for licor in licor_names:
        for plant_tag in df_per_licor[licor].keys():
            plant_df = df_per_licor[licor][plant_tag]
            if (plant_df["21 m/z"] <= 1760).any() or \
                (plant_df["21 m/z"] >= 2640).any() or \
                (plant_df["PC_Pressure"] <= 320).any() or \
                (plant_df["PC_Pressure"] >= 480).any():
                abnormal_tags.append(plant_tag)
    
    # Take the average for all gas concentration columns for each plant tag
    for licor in licor_names:
        for plant_tag in df_per_licor[licor].keys():
            df_per_licor[licor][plant_tag] = remove_non_gas_columns(df_per_licor[licor][plant_tag]).mean()
        
    # List of all plant tags
    plant_tags = []
    
    # Dataframe containing final results, with each row containing a plant rag and average values for all that plant's gas
    # concentrations
    full_df = []
    
    # Dictionary from string plant tag names to lists of strings for the gases that are above the threshold value for
    # that plant.
    plants_to_gases = {}
        
    for licor in licor_names:
        for plant_tag in df_per_licor[licor].keys():
            
            # Subtract the blank's background values from each gas's concentration
            plant_df = df_per_licor[licor][plant_tag]
            df_per_licor[licor][plant_tag] = plant_df.sub(blanks_per_licor[licor], axis=0)
            
            # Get the list of all gases above the 3 x standard deviation cutoff value for this plant
            plant_df = df_per_licor[licor][plant_tag]
            above_threshold = plant_df.gt(blank_std_per_licor[licor])
            plants_to_gases[plant_tag] = above_threshold[above_threshold == True].index.to_list()
            
            # Create individual tag csv files
            #plant_df.to_csv(plant_tag + ".csv")
            #blanks_per_licor[licor].to_csv(licor + "_blank.csv")
            #blank_std_per_licor[licor].to_csv(licor + "_std.csv")
            
            plant_tags.append(plant_tag)
            full_df.append(plant_df)
            
    # Combine list of plant dataframes into one full dataframe        
    full_df = pd.DataFrame(full_df)
    
    
    #high_variance = full_df.apply(lambda x: True if x.max() * 0.95 >= x.min() else False, axis=0)
    # We take as high varience those gases whose standard deviation over plant species is > 0.1
    high_variance = full_df.columns[(full_df.std() >= cutoff).to_list()]
    #print("Gasses with high variance")
    pd.set_option('display.max_colwidth', None)
    #print(high_variance)        
    
    # Add the list of plant tags as a new column
    full_df.insert(0, "plant_tag", plant_tags)
    
    output = {}
    output["data"] = full_df.to_csv()
    output["abnormal"] = abnormal_tags
    output["above_threshold"] = plants_to_gases
    output["high_variance"] = high_variance.to_list()
    
    # Create the output csv file.
    return output
        
def load_files(data, sheet, metadata):
    '''
    Load the files of LICOR data into data frames
    
    Args:
        data_file String path to the xlsx format data file
        sheet_name String sheet name where data_file has the data saved
        meradata_file String path to the xlsx format metadata file.
    Return
        Two dataframes, the first with the contents of the data file and the second with the contents of the metadata file
    '''
    
    # Read in the data file and specify the sheet name the data is under
    df = pd.read_excel(data, sheet_name=sheet)
    
    # Read the metadata file
    metadata = pd.read_excel(metadata)
    
    return df, metadata
    

def remove_non_gas_columns(df):
    '''
    Remove the columns from the given df that aren't the values for gas concentrations.
    
    Args:
        df Dataframe to remove non gas columns from
    Return:
        A Dataframe containing only the gas columns from df
    '''
    
    df = df.drop("time_string", axis=1)
    df = df.drop("time_number", axis=1)
    df = df.drop("21 m/z", axis=1)      
    df = df.drop("PC_Pressure", axis=1)
    df = df.drop("Mpvalve", axis=1)
    df = df.drop("DO1", axis=1)
    return df

if __name__ == "__main__":
    

    # Add argument parsing for the command and the configuration file
    parser = argparse.ArgumentParser()
    parser.add_argument("data", type=str, default="2024_05_09_T6261_Complete.xlsx")
    parser.add_argument("sheet", type=str, default="TS_all_ppbV")
    parser.add_argument("metadata", type=str, default="2024_05_09_Metadata.xlsx")
    parser.add_argument("cutoff", type=float, default=0.1)
    args = parser.parse_args()    
    
    data, metadata = load_files(args.data, args.sheet, args.metadata)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_columns', None)

    output = data_reduction(data, metadata, args.cutoff)
    with open("out.csv", "w") as out_file:
        out_file.write(output["data"])
        
    print("Abnormal conditions detected for the following plants:")
    print(output["abnormal"])
    for plant in output["above_threshold"]:
        print("\nGases above threshold detected for plant " + plant)
        print(output["above_threshold"][plant])
    print("\nGases with high variance over different plant species")
    print(output["high_variance"])
