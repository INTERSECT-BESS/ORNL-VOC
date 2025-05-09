import json
import os

import matplotlib.pyplot as plt
import pandas as pd

from io import StringIO
from typing import Dict, Union

from intersect_sdk import (
    default_intersect_lifecycle_loop,
    HierarchyConfig,
    IntersectBaseCapabilityImplementation,
    IntersectDataHandler,
    IntersectService,
    IntersectServiceConfig,
    intersect_message,
    intersect_status,
)

from dac import data_reduction

class DACCapability(IntersectBaseCapabilityImplementation):
    """
    Capability to run DAC data processing.
    """

    intersect_sdk_capability_name = "BESSDDAC"

    @intersect_message()
    def perform_data_reduction(self, params: Dict[str, str]) -> str:
        """
        Reduce the input data into a file of average values for each gas concentration, and lists of each plant tag that
        experienced unusual circumstances, each gas for each plant tag that was above the defined threshold, and each
        gas that varied over phenotypes.

        Args:
            params: Dictionary of "data" to data file contents and metadata to metadata file contents.
        Return:
            JSON string of a dictionary of "data" to output file contents, "abnormal" to a list of tags with abnormal 
            readings, "above_threshold" for a dictionary of plant tags to gases above the threshold, and "high_variance" 
            for a list of gases that had high variance over plant tags.
        """

        # Read in the data file and specify the sheet name the data is under
        df = pd.read_excel(params["data"], sheet_name=params["sheet"])
        
        # Read the metadata file
        metadata = pd.read_excel(params["metadata"])

        return json.dumps(data_reduction(data, metadata, params["cutoff"]))

    @intersect_status()
    def status(self) -> str:
        """
        Return the status Up at all times, as service is never unavailable when running.

        Return:
            String 'Up' for the INTERSECT up status.
        """

        return "Up"


if __name__ == "__main__":

    from_config_file = {
        "data_stores": {
            "minio": [
                {
                    "host": "10.64.193.144",
                    "username": "treefrog",
                    "password": "XSD#n6!&Ro4&fjrK",
                    "port": 30020,
                },
            ],
        },
        "brokers": [
            {
                "host": "10.64.193.144",
                "username": "postman",
                "password": "ZTQ0YjljYTk0MTBj",
                "port": 30011,
                "protocol": "mqtt3.1.1",
            },
        ],
    }

    # Define the service
    config = IntersectServiceConfig(
        hierarchy=HierarchyConfig(
            organization="oak-ridge-national-laboratory",
            facility="none",
            system="bessd-pilot",
            subsystem="dac",
            service="data-reduction",
        ),
        **from_config_file,
    )

    capability = DACCapability()
    capability.capability_name = "data_reduction"
    service = IntersectService([capability], config)

    print("DAC service started.")
    default_intersect_lifecycle_loop(
        service,
    )
    