# Documentation : https://www.chirpstack.io/docs/chirpstack/api/api.html

import csv
import json
import os
import logging
import argparse

import grpc
from chirpstack_api import api


"""
Appel du programme : 
dev_import -a server --token TOKEN -f csv_filename.csv -t tenant_name -a application_name -d device_profile_name

server : adresse ip chirpstack server ex 192.168.141.110:8080
token :  key chirpstack serveur
csv_filename must contain : DEVICE_ID,DEV_EUI,APP_KEY,JOIN_EUI
tenant_name : name of the tenant inside Chirpstack
application_name : name of the application
device_profile_name : name of the device profile
"""


def check_device_file(file_path):
    logging.debug("Check file")
    if not os.path.isfile(file_path):
        print(f"Error: File '{file_path}' not found.")
        return False
    try:

        with open(file_path, "r", encoding="utf-8-sig") as monfile:

            mycsv_dict = csv.DictReader(monfile, delimiter=",")

            nb_row = 0
            for row in mycsv_dict:
                if row.get("DEVICE_ID") == None or row.get("DEVICE_ID") == "":
                    print(
                        f"Error file doesn't have a correct value for DEVICE_ID in row {nb_row}"
                    )
                    exit(2)

                if row.get("DEV_EUI") == None or len(row.get("DEV_EUI")) != 16:
                    print(
                        f"Error file doesn't have a correct value for DEV_EUI in row {nb_row}"
                    )
                    exit(2)

                if row.get("APP_KEY") == None or len(row.get("APP_KEY")) != 32:
                    print(
                        f"Error file doesn't have a correct value for APP_KEY in row {nb_row}"
                    )
                    exit(2)

                if row.get("APP_EUI") == None or len(row.get("APP_EUI")) != 16:
                    print(
                        f"Error file doesn't have a correct value for APP_EUI in row {nb_row}"
                    )
                    exit(2)
                nb_row += 1

            if nb_row == 0:
                print("Be careful the file {} is empty".format(file_path))
                exit(2)

    except Exception as e:
        print("ERROR : {}".format(e))
        exit(2)


def check_downlink_file(file_path):
    if not os.path.isfile(file_path):
        print(f"Error: File '{file_path}' not found.")
        return False
    try:

        with open(file_path, "r", encoding="utf-8-sig") as monfile:

            mycsv_dict = csv.DictReader(monfile, delimiter=",")

            nb_row = 0
            for row in mycsv_dict:

                nb_row += 1

            if nb_row == 0:
                print("Be careful the file {} is empty".format(file_path))
                exit(2)

    except Exception as e:
        print("ERROR : {}".format(e))
        exit(2)


def get_tenant_id(channel, tenant_name) -> str:
    """
    Return tenant_id if the tenant exists
    Print a list of available tenants if the tenant isn't found
    """
    client = api.TenantServiceStub(channel)

    req = api.ListTenantsRequest()
    req.limit = 1000

    resp = client.List(req, metadata=auth_token)

    tenants_name = []

    for tenant in resp.result:
        tenants_name.append(tenant.name)
        if tenant.name == tenant_name:
            logging.debug(
                "Tenant named {} is present id : {}".format(tenant_name, tenant.id)
            )
            return tenant.id

    logging.debug(f"Tenant {tenant_name} not found")
    print(f"Tenant {tenant_name} not found")
    print(f"The tenents present in the server are {tenants_name}")
    exit(2)


def test_get_tenant_id(channel, tenant_name):
    tenant_id = get_tenant_id(channel, tenant_name)
    print(f"Tenant ID of {tenant_name} is {tenant_id}")


def get_application_id(channel, application_name, tenant_id) -> str:
    """
    Return True or False if the application exists
    Return List of all available application
    """
    client = api.ApplicationServiceStub(channel)

    req = api.ListApplicationsRequest()
    req.tenant_id = tenant_id
    req.limit = 1000

    resp = client.List(req, metadata=auth_token)

    applications_name = []

    for application in resp.result:
        applications_name.append(application.name)
        if application.name == application_name:
            logging.debug(
                "Application named {} is present id : {}".format(
                    application_name, application.id
                )
            )
            return application.id

    logging.debug(f"Application {application_name} not found")
    print(f"Application {application_name} not found")
    print(f"The applications present in the server are {applications_name}")
    exit(2)


def test_get_application_id(channel, application_name, tenant_id):
    application_id = get_application_id(channel, application_name, tenant_id)
    print(f"Application ID of {application_name} is {application_id}")


def get_profile_id(channel, profile_name, tenant_id) -> str:
    """
    Return True or False if the device profile exists
    Return List of all available device profile
    """
    client = api.DeviceProfileServiceStub(channel)

    req = api.ListDeviceProfilesRequest()
    req.limit = 1000
    req.tenant_id = tenant_id

    resp = client.List(req, metadata=auth_token)

    profiles_name = []

    for device_profile in resp.result:
        profiles_name.append(device_profile.name)
        if device_profile.name == profile_name:
            logging.debug(
                "Device_profile named {} is present id : {}".format(
                    profile_name, device_profile.id
                )
            )
            return device_profile.id

    logging.debug(f"Device profile {profile_name} not found")
    print(f"Device profile {profile_name} not found")
    print(f"The device profile present in the server are {profiles_name}")
    exit(2)


def test_get_profile_id(channel, profile_name, tenant_id):
    profile_id = get_profile_id(channel, profile_name, tenant_id)
    print(f"Device_profile ID of {profile_name} is {profile_id}")


def get_device_list(file_path) -> list:
    keys_to_keep = ["DEVICE_ID", "DEV_EUI", "APP_KEY", "APP_EUI", "DESCRIPTION"]
    filtered_rows = []

    with open(file_path, mode="r") as file:
        csvreader = csv.DictReader(file)
        for row in csvreader:
            filtered_row = {key: row[key] for key in keys_to_keep if key in row}
            filtered_rows.append(filtered_row)
    return filtered_rows


def show_first_lines_dict(rows, start_index, total_devices):
    rows_text = "\nHere is how the file looks like:\n"
    line_nb = 0
    for row in rows:
        if line_nb + 1 > start_index:
            if line_nb < total_devices:
                for key, value in row.items():
                    rows_text += f" | {key}: {value}"
                rows_text += "\n"
            else:
                break
        line_nb += 1
    print(rows_text)
    return rows_text


def add_devices(
    channel,
    device_file_path,
    application_id,
    device_profile_id,
    device_status
):
    """
    downlink_file={'send_downlink:' ', downlink_content':'','downlink_port':'', 'downlink_confirmed':''}
    send_downlink : bool
    downlink_content :
    downlink_port :
    downlink_confirmed : bool
    """
    client = api.DeviceServiceStub(channel)

    devices_list = get_device_list(device_file_path)

    for devices_info in devices_list:
        print(devices_info)
        device = api.Device(
            dev_eui=devices_info["DEV_EUI"],
            name=devices_info["DEVICE_ID"],
            description=devices_info["DESCRIPTION"],
            application_id=application_id,
            device_profile_id=device_profile_id,
            is_disabled=device_status,
            join_eui=devices_info["APP_EUI"],
        )

        device_keys = api.DeviceKeys(
            dev_eui=devices_info["DEV_EUI"], nwk_key=devices_info["APP_KEY"]
        )

        try:
            client.Create(
                request=api.CreateDeviceRequest(device=device), metadata=auth_token
            )
        except grpc._channel._InactiveRpcError as e:
            print(f"Failed to create device {devices_info['DEVICE_ID']}")
            print(e)

        try:
            client.CreateKeys(
                api.CreateDeviceKeysRequest(device_keys=device_keys),
                metadata=auth_token,
            )
        except grpc._channel._InactiveRpcError as e:
            print(f"Failed to set device key for {devices_info['DEVICE_ID']}")
            print(e)


def send_downlink(client, device_file_path, downlink_file_path):
    
    client = api.DeviceServiceStub(channel)

    devices_list = get_device_list(device_file_path)

    for device_info in devices_list:
        with open(downlink_file_path, "r", encoding="utf-8-sig") as file:
            data = json.load(file)
        
            for bloc in data:
                try:
                    # Extract the values from each block
                    downlink_name = bloc.get("name")
                    downlink_port = int(bloc.get("port"))
                    downlink_content = bloc.get("data")
                    downlink_confirmed = bloc.get("confirmed")

                    print(downlink_port)
                    print(downlink_content)
                    print(downlink_confirmed)


                    # Construct the request
                    req = api.EnqueueDeviceQueueItemRequest()
                    req.queue_item.confirmed = downlink_confirmed
                    req.queue_item.data = bytes.fromhex(downlink_content)
                    req.queue_item.f_port = downlink_port
                    req.queue_item.dev_eui = device_info['DEV_EUI']

                    client.Enqueue(req, metadata=auth_token)
                    print(f"Successfully enqueued downlink for {downlink_name}")

                except grpc._channel._InactiveRpcError as e:
                    print(f"Failed to enqueue downlink for {downlink_name}")
                    print(e)
                except Exception as e:
                    print(f"An error occurred while processing downlink for {downlink_name}")
                    print(e)


def delete_devices(channel, device_file_path):

    client = api.DeviceServiceStub(channel)

    devices_list = get_device_list(device_file_path)

    for devices_info in devices_list:
        dev_eui = devices_info["DEV_EUI"]

        try:
            client.Delete(
                request=api.DeleteDeviceRequest(dev_eui=dev_eui), metadata=auth_token
            )

        except grpc._channel._InactiveRpcError as e:
            print(f"Failed to delete device {devices_info['DEVICE_ID']}")
            print(e)


def getArgs(argv=None):
    parser = argparse.ArgumentParser(
        prog="Chirpstack device Provisionning",
        description="""Import Device from a csv file
        CSV File header : DEVICE_ID,DEV_EUI,APP_KEY,JOIN_EUI,DESCRIPTION
        """,
        epilog="Thanks for using",
    )
    parser.add_argument(
        "-f",
        dest="device_list_file_path",
        required=True,
        help="The csv filename that contains device info",
    )
    parser.add_argument(
        "-t",
        dest="api_token",
        required=True,
        help="API TOKEN Created in Chirpstack",
    )
    parser.add_argument(
        "-s", dest="server", required=True, help="IP_address:port of the server"
    )

    parser.add_argument(
        "--action",
        required=True,
        dest="action",
        choices=["add", "add&downlink", "downlink", "remove"],
        help="The action to perfom. action=add -> add device(s), action=add&downlink -> add device(s) and send downlink, action=downlink -> send downlink, action=remove -> remove device(s)",
    )
    parser.add_argument(
        "--tenant",
        required=True,
        dest="tenant_name",
        help="The name of the tenant where the devices will be added",
    )
    parser.add_argument(
        "--app",
        required=True,
        dest="application_name",
        help="The name of the application.",
    )
    parser.add_argument(
        "--dev_profile",
        required=True,
        dest="device_profile_name",
        help="Name of the device profile to use when creating device",
    )
    parser.add_argument(
        "--downlink_file",
        dest="downlink_file_path",
        help="The full path of the downlink json file. If not specified, no downlink will be sent",
    )

    # parser.add_argument(
    #     "--debug",
    #     dest="debug",
    #     choices=["info", "debug", "none"],
    #     help="debug value, if not specified no debug",
    # )
    return parser


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    args = vars(getArgs().parse_args())
    logging.debug("args : {}".format(args))
    api_token = args["api_token"]
    server = args["server"]
    action = args["action"]
    tenant_name = args["tenant_name"]
    application_name = args["application_name"]
    device_profile_name = args["device_profile_name"]
    downlink_file_path = args.get("downlink_file_path")
    device_list_file_path = args["device_list_file_path"]

    # Connect without using TLS.
    channel = grpc.insecure_channel(server)

    # Define the API key meta-data.
    auth_token = [("authorization", "Bearer %s" % api_token)]

    ##### Check_file
    check_device_file(device_list_file_path)

    ##### Get_ID
    tenant_id = get_tenant_id(channel, tenant_name)

    application_id = get_application_id(channel, application_name, tenant_id)

    profile_id = get_profile_id(channel, device_profile_name, tenant_id)

    ##### Action
    if action == "remove":
        delete_devices(channel, device_list_file_path)

    elif action == "add" or action == "add&downlink":
        add_devices(channel, device_list_file_path, application_id, profile_id, False)

    if action == "add&downlink" or action == "downlink":
        send_downlink(channel, device_list_file_path, downlink_file_path,)