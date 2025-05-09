import dac

from active.strategy.decorators import ActiveStrategy

@ActiveStrategy("DAC Strategy")
class DACStrategy():
    '''
    Strategy for running the DAC analysis. Liscencing for the software that produces the input files currently 
    prevents full automation. This Strategy exists solely so that results can be displayed in the ACTIVE Workbench
    GUI.
    
    Note: requires ACTIVE (open source release forthcoming) installation to run.
    '''
    
    def __init__(self, cutoff_value, data_file, data_store, metadata_file, sheet_name):
        '''
        Default constructor.
        
        Prototype ACTIVE Environment file definition:
        {
            "type": "DAC Strategy",
            "parameters": {
                "cutoff_value": 0.1,
                "data_file": "data.xlsx",
                "data_store": "!ACTIVE:My DataStore",
                "metadata_file": "metadata.xlsx",
                "sheet_name": "sheet2"
            }
        }
        
        Parameters:
            cutoff_value: Float that defines the cutoff for standard deviations over mean over all measurements for
                all plants above which a gas will be considered as "high variance".
            data_file: String path to the file containing measurement data
            data_store: DataStore to save the csv average gas per plant data.
            metadata_file: String path to the file containing measurement metadata
            sheet_name: String name for the sheet inside of data_file containing the data
        '''
        
        self.cutoff_value = cutoff_value
        self.data_file = data_file
        self.data_store = data_store
        self.metadata_file = metadata_file
        self.sheet_name = sheet_name
        
    def step(self, final_episode=False):
        '''
        Perform the entire analysis. 
        '''
        
        data, metadata = dac.load_files(self.data_file, self.sheet_name, self.metadata_file)
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_columns', None)
    
        output = dac.data_reduction(data, metadata, self.cutoff)
        
        self.data_store.save("", "out.csv", output["data"])
            
        print("Abnormal conditions detected for the following plants:")
        print(output["abnormal"])
        for plant in output["above_threshold"]:
            print("\nGases above threshold detected for plant " + plant)
            print(output["above_threshold"][plant])
        print("\nGases with high variance over different plant species")
        print(output["high_variance"])       
        
        