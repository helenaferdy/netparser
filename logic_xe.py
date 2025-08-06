import os
import re
import textfsm
import pandas as pd
import multiprocessing

# This script assumes a 'textfsm_template' directory exists alongside it.
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_TEXTFSM = os.path.join(SCRIPT_DIR, "textfsm_template")

def execute_split(input_folder, output_dir):
    # This function is working correctly and remains unchanged.
    print(f"--- Splitting IOS-XE logs from: {input_folder} ---")
    if not os.path.isdir(input_folder):
        return [f"Input directory not found: {input_folder}"]
    error_messages = []
    dir_list = os.listdir(input_folder)
    filenames = [os.path.splitext(f)[0] for f in dir_list if os.path.isfile(os.path.join(input_folder, f))]
    for name_file in filenames:
        try:
            with open(os.path.join(input_folder, f"{name_file}.txt"), "r", encoding="utf-8-sig") as f: data = f.read()
        except UnicodeDecodeError:
            with open(os.path.join(input_folder, f"{name_file}.txt"), "r", encoding="windows-1252") as f: data = f.read()
        except FileNotFoundError: continue
        # data = re.sub(r"^show.*\n|^!$\n|terminal no length", "", data, flags=re.MULTILINE)
        blocks = re.split(rf"({name_file}#show .+?\n)", data)
        os.makedirs(output_dir, exist_ok=True)
        device_output_dir = os.path.join(output_dir, name_file)
        os.makedirs(device_output_dir, exist_ok=True)

        for i in range(1, len(blocks), 2):
            header = blocks[i].strip()
            command = header.split("#show ")[-1].replace(" ", "_")
            if command == "etherchannel_summary": command = "port-channel_summary"
            elif command == "interfaces_description": command = "interface_description"
            elif command == "interfaces_status": command = "interface_status"
            elif command == "interfaces_trunk": command = "interface_trunk"
            # content = header + "\n" + blocks[i+1]
            content = blocks[i+1]
            content = re.sub(r"^!$\n|^terminal no length\n|^Stop recording.*\n?", "", content, flags=re.MULTILINE)
            command = command.replace(f"{name_file}#_", "")
            filename = os.path.join(device_output_dir, f"show_{command}.txt")
            with open(filename, "w", encoding='utf-8') as out: out.write(content)
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

def process_single_hostname(args):
    """
    Worker function with detailed debugging to process a single IOS-XE device.
    """
    hostname, source_data_path, output_folder = args
    print(f"[{hostname}] Starting processing...")

    def read_file(filename):
        path = os.path.join(source_data_path, hostname, filename)
        try:
            return open(path, 'r', encoding='utf-8').read()
        except FileNotFoundError:
            print(f"[{hostname}][DEBUG] File not found: {filename}")
            return ""

    # --- DEBUGGING WRAPPER FOR FILE PARSING ---
    def parse_and_create_df(file_to_parse, template_name, column_map=None):
        df_name = file_to_parse.replace('.txt', '')
        try:
            print(f"[{hostname}][DEBUG] Parsing: {file_to_parse}")
            df = pd.DataFrame(parsing_plain_text_to_json(template_name, read_file(file_to_parse)))
            if df.empty:
                print(f"[{hostname}][DEBUG] No data parsed from {file_to_parse}.")
                return df
            
            if column_map:
                df.rename(columns=column_map, inplace=True)
            
            print(f"[{hostname}][DEBUG] Successfully parsed {df_name}. Found {len(df)} rows. Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"[{hostname}][FAILED] Could not parse or process '{df_name}'. Error: {e}")
            return pd.DataFrame() # Return empty DataFrame on failure

    # --- PARSE ALL FILES WITH DEBUGGING ---
    mac_address = parse_and_create_df("show_mac_address-table.txt", "cisco_ios_show_mac-address-table.textfsm", {'DESTINATION_ADDRESS': 'MAC_ADDRESS', 'DESTINATION_PORT': 'PORTS'})
    ip_arp = parse_and_create_df("show_ip_arp.txt", "cisco_ios_show_ip_arp.textfsm")
    interface_description = parse_and_create_df("show_interface_description.txt", "cisco_ios_show_interfaces_description.textfsm")
    inventory = parse_and_create_df("show_inventory.txt", "cisco_ios_show_inventory.textfsm")
    port_channel_summary = parse_and_create_df("show_port-channel_summary.txt", "cisco_ios_show_etherchannel_summary.textfsm")
    interface_status = parse_and_create_df("show_interface_status.txt", "cisco_ios_show_interfaces_status.textfsm")
    cdp_neighbors = parse_and_create_df("show_cdp_neighbors.txt", "cisco_ios_show_cdp_neighbors.textfsm")
    lldp_neighbors = parse_and_create_df("show_lldp_neighbors.txt", "cisco_ios_show_lldp_neighbors.textfsm")
    interface_trunk = parse_and_create_df("show_interface_trunk.txt", "cisco_ios_show_interfaces_trunk.textfsm")

    result = []
    if mac_address.empty:
        print(f"[{hostname}] No MAC address data to process. Skipping main loop.")
        return None # Return None if no primary data exists

    print(f"[{hostname}][DEBUG] Starting to process {len(mac_address)} MAC address entries...")
    for index, row in mac_address.iterrows():
        try:
            # --- MAIN PROCESSING LOGIC FOR EACH ROW ---
            if "CPU" in row.get('PORTS', ''): continue
            
            # port = row['PORTS']
            port_list = row.get('PORTS', [])
            port = port_list[0] if port_list else ''

            # If after extraction the port is empty, skip this row.
            if not port:
                continue
            
            mac = row['MAC_ADDRESS']
            vlan = row['VLAN_ID']
            sn_array = inventory[inventory['PID'].notna()]['SN'].values if not inventory.empty else []
            sn = sn_array[0] if len(sn_array) > 0 else None

            ip_array = ip_arp.loc[ip_arp['MAC_ADDRESS'] == mac, 'IP_ADDRESS'].values if not ip_arp.empty else []
            ip = ip_array[0] if len(ip_array) > 0 else None

            desc_array = interface_description.loc[interface_description['PORT'] == port, 'DESCRIPTION'].values if not interface_description.empty else []
            desc = desc_array[0] if len(desc_array) > 0 else None

            if port.startswith('Po'):
                member_po_array = port_channel_summary.loc[port_channel_summary['BUNDLE_NAME'] == port, 'MEMBER_INTERFACE'].values if not port_channel_summary.empty else []
                member_po = ', '.join(member_po_array[0]) if len(member_po_array) > 0 else ""
                sfp_array = interface_status.loc[interface_status['PORT'] == member_po.split(',')[0], 'TYPE'].values if not interface_status.empty and member_po else []
                sfp = sfp_array[0] if len(sfp_array) > 0 else None
                type_cable = "UTP" if "T" in str(sfp) else "Fiber" if sfp else "???"
            else:
                member_po = ""
                sfp_array = interface_status.loc[interface_status['PORT'] == port, 'TYPE'].values if not interface_status.empty else []
                sfp = sfp_array[0] if len(sfp_array) > 0 else None
                type_cable = "UTP" if "T" in str(sfp) else "Fiber" if sfp and "Vlan" not in port else "???"

            trunk_access_array = interface_status.loc[interface_status['PORT'] == port, 'VLAN_ID'].values if not interface_status.empty else []
            trunk_access = trunk_access_array[0] if len(trunk_access_array) > 0 else ""
            if len(trunk_access) > 0 and "trunk" not in trunk_access and "routed" not in trunk_access: trunk_access = "Access"

            cdp_remote_hostname, cdp_remote_platform, cdp_remote_port = None, None, None
            if not cdp_neighbors.empty:
                interfaces_to_check = member_po.split(', ') if port.startswith('Po') and member_po else [port]
                cdp_data = cdp_neighbors[cdp_neighbors['LOCAL_INTERFACE'].isin(interfaces_to_check)]
                if not cdp_data.empty:
                    cdp_remote_hostname = cdp_data.iloc[0]['NEIGHBOR_NAME']
                    cdp_remote_platform = cdp_data.iloc[0]['PLATFORM']
                    cdp_remote_port = ', '.join(cdp_data['NEIGHBOR_INTERFACE'].unique())
            
            lldp_remote_hostname, lldp_remote_port = None, None
            if not lldp_neighbors.empty and not port.startswith('Po'):
                lldp_array = lldp_neighbors.loc[lldp_neighbors['LOCAL_INTERFACE'] == port, ['NEIGHBOR_NAME', 'NEIGHBOR_INTERFACE']].values
                if len(lldp_array) > 0: lldp_remote_hostname, lldp_remote_port = lldp_array[0]

            allowed_vlan_trunk = ''
            if not interface_trunk.empty:
                allowed_vlan_trunk_values = interface_trunk.loc[interface_trunk['PORT'] == port, 'ALLOWED_VLANS'].values
                if len(allowed_vlan_trunk_values) > 0 and allowed_vlan_trunk_values[0]:
                    allowed_vlan_trunk = allowed_vlan_trunk_values[0] if isinstance(allowed_vlan_trunk_values[0], str) else ','.join(allowed_vlan_trunk_values[0])
            
            result.append({
                'Hostname': hostname, 'Serial Number': sn, 'Port': port, "Member PO": member_po,
                'Tipe Kabel': type_cable, 'SFP': sfp, 'IP Address': ip, 'Mac Address': mac, 'VLAN': vlan,
                'Allowed VLAN': allowed_vlan_trunk, 'Trunk/Access': trunk_access, 'Description': desc,
                'CDP Neighbor Hostname': cdp_remote_hostname, 'CDP Neighbor Platform': cdp_remote_platform,
                'CDP Neighbor Port': cdp_remote_port, 'LLDP Neighbor Hostname': lldp_remote_hostname, 'LLDP Neighbor Port': lldp_remote_port
            })
        except Exception as e:
            # This will catch errors on a specific row and report them without crashing
            print(f"[{hostname}][FAILED] Skipped processing row #{index + 1} due to an error. MAC: {row.get('MAC_ADDRESS', 'N/A')}, Port: {row.get('PORTS', 'N/A')}. Error: {e}")
            continue

    if not result:
        print(f"[{hostname}] No results generated after processing all rows.")
        return None

    result_final = pd.DataFrame(result)
    output_path = os.path.join(output_folder, f"{hostname}.xlsx")

    with pd.ExcelWriter(output_path) as writer:
        result_final.to_excel(writer, sheet_name='Final Data', index=False)
    
    print(f"[{hostname}] Successfully created output file.")
    return f"{hostname}.xlsx"


def execute_main(input_folder, output_folder):
    """
    Dispatcher function that processes all devices in parallel.
    """
    # This function remains identical.
    SOURCE_DATA = input_folder
    if not os.path.isdir(SOURCE_DATA):
        print(f"Error: Source data directory not found at {SOURCE_DATA}")
        return
    hostnames = [h for h in os.listdir(SOURCE_DATA) if os.path.isdir(os.path.join(SOURCE_DATA, h))]
    tasks = [(hostname, SOURCE_DATA, output_folder) for hostname in hostnames]
    with multiprocessing.Pool() as pool:
        results = pool.map(process_single_hostname, tasks)
    print("\n--- Parallel processing complete. Starting final aggregation. ---")
    all_device_data = []
    for filename in results:
        if filename:
            try:
                filepath = os.path.join(output_folder, filename)
                df_single = pd.read_excel(filepath, sheet_name='Final Data')
                all_device_data.append(df_single)
            except Exception as e:
                print(f"Could not read {filename} for aggregation, error: {e}")
    if not all_device_data:
        print("No data was aggregated. Cannot create final report.")
        return
    final_df = pd.concat(all_device_data, ignore_index=True)
    if 'IP Address' in final_df.columns and 'Mac Address' in final_df.columns:
        mac_to_ip = final_df.dropna(subset=['IP Address']).set_index('Mac Address')['IP Address'].to_dict()
        final_df['IP Address'] = final_df['IP Address'].fillna(final_df['Mac Address'].map(mac_to_ip))
    updated_report_path = os.path.join(output_folder, 'Final_Report.xlsx')
    final_df.to_excel(updated_report_path, index=False)
    print(f"Successfully created '{updated_report_path}'")