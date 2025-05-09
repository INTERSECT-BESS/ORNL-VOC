import argparse
import json
import pandas as pd

from dac import load_files

from intersect_sdk import (
    INTERSECT_JSON_VALUE,
    IntersectClient,
    IntersectClientCallback,
    IntersectClientConfig,
    IntersectDirectMessageParams,
    default_intersect_lifecycle_loop,
)


def simple_client_callback(
    _source: str, _operation: str, _has_error: bool, payload: INTERSECT_JSON_VALUE
) -> None:
    """This simply prints the response from the service to your console.

    As we don't want to engage in a back-and-forth, we simply throw an exception to break out of the message loop.
    Ways to continue listening to messages or sending messages will be explored in other examples.

    Params:
      _source: the source of the response message. In this case it will always be from the hello_service.
      _operation: the name of the function we called in the original message. In this case it will always be "say_hello_to_name".
      _has_error: Boolean value which represents an error. Since there is never an error in this example, it will always be "False".
      payload: Value of the response from the Service. The typing of the payload varies, based on the operation called and whether or not
        _has_error was set to "True". In this case, since we do not have an error, we can defer to the operation's response type. This response type is
        "str", so the type will be "str". The value will always be "Hello, hello_client!".

        Note that the payload will always be a deserialized Python object, but the types are fairly limited: str, bool, float, int, None, List[T], and Dict[str, T]
        are the only types the payload can have. "T" in this case can be any of the 7 types just mentioned.
    """

    with open("out.csv", "w") as out_file:
        out_file.write(payload["data"])
        
    print("Abnormal conditions detected for the following plants:")
    print(payload["abnormal"])
    for plant in payload["above_threshold"]:
        print("\nGases above threshold detected for plant " + plant)
        print(payload["above_threshold"][plant])
    print("\nGases with high variance over different plant species")
    print(payload["high_variance"])
    print("\nSaved output to out.csv")
    
    # raise exception to break out of message loop - we only send and wait for one message
    raise Exception


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


    # Add argument parsing for the command and the configuration file
    parser = argparse.ArgumentParser()
    parser.add_argument("data", nargs='*', default="2024_05_09_T6261_Complete.xlsx")
    parser.add_argument("sheet", nargs='*', default="TS_all_ppbV")
    parser.add_argument("metadata", nargs='*', default="2024_05_09_Metadata.xlsx")
    parser.add_argument("cutoff", type=float, nargs='*', default=0.1)
    args = parser.parse_args()    
    
    params["sheet"] = args.sheet
    
    with open(args.data, 'r') as file:
        params["data"] = file.read()
    
    with open(args.metadata, 'r') as file:
        params["metadata"] = file.read()    

    initial_messages = [
        IntersectDirectMessageParams(
            destination="oak-ridge-national-laboratory.none.bessd-pilot.dac.data-reduction",
            operation="BESSDDAC.data_reduction",
            payload=params,
        )
    ]

    config = IntersectClientConfig(
        initial_message_event_config=IntersectClientCallback(
            messages_to_send=initial_messages
        ),
        **from_config_file,
    )

    """
    step three: create the client.

    We also need a callback to handle incoming user messages.
    """
    client = IntersectClient(config=config, user_callback=simple_client_callback)

    """
    step four - start lifecycle loop. The only necessary parameter is your client.
    with certain applications (i.e. REST APIs) you'll want to integrate the client in the existing lifecycle,
    instead of using this one.
    In that case, just be sure to call client.startup() and client.shutdown() at appropriate stages.
    """
    default_intersect_lifecycle_loop(
        client,
    )
