import os
import re
import textfsm
import pandas as pd
from time import sleep, perf_counter

# This script assumes a 'textfsm_template' directory exists alongside it.
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_TEXTFSM = os.path.join(SCRIPT_DIR, "textfsm_template")

def execute_split(input_folder, output_dir):
    """
    Validates and splits raw log files into individual command outputs.
    Returns a list of error messages if validation fails.
    """
    print(f"--- Splitting logs from: {input_folder} ---")
    if not os.path.isdir(input_folder):
        return [f"Input directory not found: {input_folder}"]
        
    required_commands = [
        'show interface description', 'show interface status', 'show interface trunk',
        'show port-channel summary', 'show ip arp', 'show mac address-table',
        'show inventory', 'show cdp neighbors', 'show lldp neighbors'
    ]
    error_messages = []
    dir_list = os.listdir(input_folder)
    filenames = [os.path.splitext(f)[0] for f in dir_list if os.path.isfile(os.path.join(input_folder, f))]

    for name_file in filenames:
        try:
            with open(os.path.join(input_folder, f"{name_file}.txt"), "r", encoding="utf-8-sig") as f:
                data = f.read()
        except UnicodeDecodeError:
            with open(os.path.join(input_folder, f"{name_file}.txt"), "r", encoding="windows-1252") as f:
                data = f.read()
        except FileNotFoundError:
            continue

        # --- VALIDATION LOGIC ---
        missing_commands = []
        for command in required_commands:
            # Check if the command string appears in the log file content
            if command not in data:
                missing_commands.append(f"'{command}'")
        
        if missing_commands:
            error_message = f"File '{name_file}.txt' is missing command(s): {', '.join(missing_commands)}."
            error_messages.append(error_message)
            continue # Skip this file and check the next one

        # --- SPLITTING LOGIC (if validation passes) ---
        data = re.sub(r"^show.*\n|^!$\n|terminal no length", "", data, flags=re.MULTILINE)
        blocks = re.split(rf"({name_file}# show .+?\n)", data)
        
        os.makedirs(output_dir, exist_ok=True)
        device_output_dir = os.path.join(output_dir, name_file)
        os.makedirs(device_output_dir, exist_ok=True)

        for i in range(1, len(blocks), 2):
            header = blocks[i].strip()
            command = header.split("#show ")[-1].replace(" ", "_")
            content = header + "\n" + blocks[i+1]
            command = command.replace(f"{name_file}#_", "")
            filename = os.path.join(device_output_dir, f"{command}.txt")
            with open(filename, "w", encoding='utf-8') as out:
                out.write(content)
        print("Files saved in:", device_output_dir)
        
    return error_messages


def parsing_plain_text_to_json(template_file, data):
    """Helper function to parse text using a TextFSM template."""
    try:
        with open(os.path.join(TEMPLATE_TEXTFSM, template_file)) as template:
            fsm = textfsm.TextFSM(template)
            return fsm.ParseTextToDicts(data)
    except FileNotFoundError:
        print(f"Warning: Template '{template_file}' not found.")
        return []

def execute_main(input_folder, output_folder):
    # This is the exact function provided by the user, adapted for Flask.
    # `input_folder` is the full path to the 'split' directory.
    # `output_folder` is the full path to the final 'output' directory.
    SOURCE_DATA = input_folder
    
    if not os.path.isdir(SOURCE_DATA):
        print(f"Error: Source data directory not found at {SOURCE_DATA}")
        return

    list_dir = os.listdir(SOURCE_DATA)
    for hostname in list_dir:
        if not os.path.isdir(os.path.join(SOURCE_DATA, hostname)):
            continue
        print (hostname)

        # Helper to read file content safely
        def read_file(filename):
            path = os.path.join(SOURCE_DATA, hostname, filename)
            try:
                return open(path, 'r', encoding='utf-8').read()
            except FileNotFoundError:
                return ""

        mac_address                 = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_mac_address-table.textfsm", read_file("show_mac_address-table.txt")))
        ip_arp                      = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_ip_arp.textfsm", read_file("show_ip_arp.txt")))
        interface_description       = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_interface_description.textfsm", read_file("show_interface_description.txt")))
        inventory                   = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_inventory.textfsm", read_file("show_inventory.txt")))
        port_channel_summary        = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_port-channel_summary.textfsm", read_file("show_port-channel_summary.txt")))
        interface_status            = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_interface_status.textfsm", read_file("show_interface_status.txt")))
        cdp_neighbors               = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_cdp_neighbors.textfsm", read_file("show_cdp_neighbors.txt")))
        lldp_neighbors               = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_lldp_neighbors.textfsm", read_file("show_lldp_neighbors.txt")))
        interface_trunk               = pd.DataFrame(parsing_plain_text_to_json("cisco_nxos_show_interface_trunk.textfsm", read_file("show_interface_trunk.txt")))

        result = []
        if mac_address.empty:
            print(f"No MAC address data for {hostname}, skipping.")
            continue

        for index, row in mac_address.iterrows():
            if "sup" or "vPC" not in row['PORTS']:
                port = row['PORTS']
                mac = row['MAC_ADDRESS']
                vlan = row['VLAN_ID']
                
                sn_array = inventory[inventory['NAME'] == 'Chassis']['SN'].values if not inventory.empty else []
                sn = sn_array[0] if len(sn_array) > 0 else None

                if ip_arp.empty:
                    ip = None
                else:
                    ip_array = ip_arp.loc[ip_arp['MAC_ADDRESS'] == mac, 'IP_ADDRESS'].values
                    ip = ip_array[0] if len(ip_array) > 0 else None

                desc_array = interface_description.loc[interface_description['PORT'] == port, 'DESCRIPTION'].values if not interface_description.empty else []
                desc = desc_array[0] if len(desc_array) > 0 else None

                if re.match(r'^Po', port):
                    member_po_array = port_channel_summary.loc[port_channel_summary['BUNDLE_NAME'] == port, 'MEMBER_INTERFACE'].values if not port_channel_summary.empty else []
                    member_po_temp = member_po_array[0] if len(member_po_array) > 0 else None
                    member_po = ', '.join(member_po_temp) if member_po_temp else ""

                    sfp_array = interface_status.loc[interface_status['PORT'] == member_po.split(',')[0], 'TYPE'].values if not interface_status.empty and member_po else []
                    sfp = sfp_array[0] if len(sfp_array) > 0 else None

                    if type(sfp) == str:
                        type_cable = "UTP" if "T" in sfp else "Fiber"
                    else:
                        type_cable = "???"
                else:
                    member_po = ""
                    sfp_array = interface_status.loc[interface_status['PORT'] == port, 'TYPE'].values if not interface_status.empty else []
                    sfp = sfp_array[0] if len(sfp_array) > 0 else None
                    if "Vlan" in port:
                        type_cable = "???"
                    else:
                        if type(sfp) == str:
                            type_cable = "UTP" if "T" in sfp else "Fiber"
                        else:
                            type_cable = "???"

                trunk_access_array = interface_status.loc[interface_status['PORT'] ==  port, 'VLAN_ID'].values if not interface_status.empty else []
                trunk_access = trunk_access_array[0] if len(trunk_access_array) > 0 else ""
                if len(trunk_access) != 0:
                    if "trunk" not in trunk_access and "routed" not in trunk_access:
                        trunk_access = "Access"
                    else:
                        pass

                if cdp_neighbors.empty:
                    cdp_remote_hostname, cdp_remote_platform, cdp_remote_port = None, None, None
                else:
                    if re.match(r'^Po', port):
                        cdp_remote_hostname, cdp_remote_platform, cdp_remote_port = '', '', ''
                        for intf in member_po.split(', '):
                            if not intf: continue
                            cdp_array = cdp_neighbors.loc[cdp_neighbors['LOCAL_INTERFACE'] == intf, ['NEIGHBOR_NAME', 'PLATFORM', 'NEIGHBOR_INTERFACE']].values
                            if len(cdp_array) > 0:
                                cdp_remote_hostname = cdp_array[0][0] if cdp_array[0][0] not in cdp_remote_hostname else cdp_remote_hostname
                                cdp_remote_platform = cdp_array[0][1] if cdp_array[0][1] not in cdp_remote_platform else cdp_remote_platform
                                cdp_remote_port = cdp_remote_port + cdp_array[0][2] if len(cdp_remote_port) == 0 else cdp_remote_port + f" ,{cdp_array[0][2]}"
                    else:
                        cdp_array = cdp_neighbors.loc[cdp_neighbors['LOCAL_INTERFACE'] == port, ['NEIGHBOR_NAME', 'PLATFORM', 'NEIGHBOR_INTERFACE']].values
                        if len(cdp_array) > 0:
                            cdp_remote_hostname, cdp_remote_platform, cdp_remote_port = cdp_array[0]
                        else:
                            cdp_remote_hostname, cdp_remote_platform, cdp_remote_port = None, None, None
                
                if lldp_neighbors.empty:
                    lldp_remote_hostname, lldp_remote_port = None, None
                else:
                    if re.match(r'^Po', port):
                        lldp_remote_hostname, lldp_remote_platform, lldp_remote_port = '', '', ''
                    else:
                        lldp_array = lldp_neighbors.loc[lldp_neighbors['LOCAL_INTERFACE'] == port, ['NEIGHBOR_NAME', 'NEIGHBOR_INTERFACE']].values
                        if len(lldp_array) > 0:
                            lldp_remote_hostname, lldp_remote_port = lldp_array[0]
                        else:
                            lldp_remote_hostname, lldp_remote_port = None, None

                # --- FIX START ---
                # This block correctly handles the 'ALLOWED_VLANS' value.
                allowed_vlan_trunk = ''
                if not interface_trunk.empty:
                    allowed_vlan_trunk_values = interface_trunk.loc[interface_trunk['PORT'] == port, 'ALLOWED_VLANS'].values
                    if len(allowed_vlan_trunk_values) > 0:
                        # The value from textfsm might be a list with one string, or just a string.
                        # This handles both cases to get the raw string correctly.
                        raw_value = allowed_vlan_trunk_values[0]
                        if isinstance(raw_value, list):
                            # If it's a list (e.g., ['vlan1', 'vlan2']), join it.
                            allowed_vlan_trunk = ','.join(raw_value)
                        else:
                            # If it's already a string (e.g., '22-23,32-33'), use it directly.
                            allowed_vlan_trunk = raw_value
                # --- FIX END ---
                
                result.append({
                    'Hostname': hostname, 'Serial Number': sn, 'Port': port, "Member PO": member_po,
                    'Tipe Kabel': type_cable, 'SFP': sfp, 'IP Address': ip, 'Mac Address': mac, 'VLAN': vlan,
                    'Allowed VLAN': allowed_vlan_trunk, 'Trunk/Access': trunk_access, 'Description': desc,
                    'CDP Neighbor Hostname': cdp_remote_hostname, 'CDP Neighbor Platform': cdp_remote_platform,
                    'CDP Neighbor Port': cdp_remote_port, 'LLDP Neighbor Hostname': lldp_remote_hostname,
                    'LLDP Neighbor Port': lldp_remote_port
                })
        
        if not result:
            print(f"No results generated for {hostname}")
            continue

        result_final = pd.DataFrame(result)
        os.makedirs(output_folder, exist_ok=True)

        with pd.ExcelWriter(os.path.join(output_folder, f"{hostname}.xlsx")) as writer:
            result_final.to_excel(writer, sheet_name='Final Data', index=False)
            mac_address.to_excel(writer, sheet_name="Mac Address", index=False)
            ip_arp.to_excel(writer, sheet_name="IP ARP", index=False)
            interface_description.to_excel(writer, sheet_name="Interface Description", index=False)
            inventory.to_excel(writer, sheet_name="Inventory", index=False)
            port_channel_summary.to_excel(writer, sheet_name="Port-Channel Summary", index=False)
            interface_status.to_excel(writer, sheet_name="Interface Status", index=False)
            cdp_neighbors.to_excel(writer, sheet_name="CDP Neighbors", index=False)
            lldp_neighbors.to_excel(writer, sheet_name="LLDP Neighbors", index=False)

        all_devices_path = os.path.join(output_folder, 'All_Devices.xlsx')
        if not os.path.exists(all_devices_path):
            with pd.ExcelWriter(all_devices_path) as writer:
                result_final.to_excel(writer, sheet_name='Final Data', index=False)
        else:
            try:
                with pd.ExcelWriter(all_devices_path, mode="a", if_sheet_exists="overlay") as writer:
                    startrow = writer.sheets['Final Data'].max_row
                    result_final.to_excel(writer, sheet_name='Final Data', index=False, startrow=startrow, header=False)
            except Exception as e:
                print(f"Could not append to Excel file, error: {e}")

        print (f"Done create output for {hostname}")

    all_devices_final_path = os.path.join(output_folder, 'All_Devices.xlsx')
    if os.path.exists(all_devices_final_path):
        try:
            df = pd.read_excel(all_devices_final_path)
            mac_to_ip = df.dropna(subset=['IP Address']).set_index('Mac Address')['IP Address'].to_dict()
            df['IP Address'] = df['IP Address'].fillna(df['Mac Address'].map(mac_to_ip))
            # --- FILENAME CHANGE ---
            df.to_excel(os.path.join(output_folder, 'Final_Report.xlsx'), index=False)
            print("Created 'Final_Report.xlsx'")
        except Exception as e:
            print(f"Could not create final updated report. Error: {e}")
