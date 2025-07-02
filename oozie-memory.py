import subprocess
import sys
import os
import xml.etree.ElementTree as ET
import re
from datetime import datetime
import argparse

def run_cmd(cmd):
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        return output.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command execution failed: {cmd}\nError:\n{e.output}")
        sys.exit(1)

def check_oozie_action_status(coord_action_id):
    cmd = f"oozie job -info {coord_action_id}"
    output = run_cmd(cmd)
    match = re.search(r"Status\s+:\s+(\w+)", output)
    if not match:
        print("Unable to determine job status. Exiting the program.")
        sys.exit(1)

    status = match.group(1).strip().upper()
    print(f"Oozie job status: {status}")

    if status != "KILLED":
        print(f"Job is currently in status: {status}. Termination required before proceeding.")
        print("Terminate the job using: oozie job -kill <coord-id>")
        sys.exit(1)

def extract_workflow_id(coord_action_id):
    cmd = f"oozie job -info {coord_action_id} | grep 'External ID' | awk '{{print $4}}'"
    return run_cmd(cmd)

def extract_hadoop_workflow_path(workflow_id):
    cmd = f"oozie job -info {workflow_id} | grep 'App Path' | awk '{{print $4}}' | sed -e 's/^.*\\/projects/\\/projects/'"
    return run_cmd(cmd)

def download_workflow_xml(hadoop_path):
    cmd = f"hadoop fs -get {hadoop_path} ."
    run_cmd(cmd)
    return os.path.basename(hadoop_path)

def backup_workflow_xml(hadoop_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{hadoop_path}_bak_{timestamp}"
    cmd = f"hadoop fs -mv {hadoop_path} {backup_path}"
    run_cmd(cmd)
    print(f"Backup successfully created at: {backup_path}")

def upload_workflow_xml(local_file, hadoop_path):
    cmd = f"hadoop fs -put {local_file} {hadoop_path}"
    run_cmd(cmd)
    print(f"Updated XML file successfully uploaded to: {hadoop_path}")

def increase_yarn_value(value, delta_mb):
    """Handles -Xmx4096M style strings or integers."""
    if value.startswith('-Xmx') and value.endswith('M'):
        num = int(value[4:-1])
        return f"-Xmx{num + delta_mb}M"
    elif value.isdigit():
        return str(int(value) + delta_mb)
    else:
        return value  # Unchanged if not matching expected format

def increase_general_value(value, delta_mb=1024, is_java_opt=False, is_reduce_memory=False):
    if value.startswith('${'):
        if is_reduce_memory:
            return f'-Xmx{delta_mb - 512}M'
        return str(delta_mb)
    elif value.isdigit():
        return str(int(value) + delta_mb)
    elif re.match(r'-Xmx\d+[mM]', value):
        match = re.match(r'-Xmx(\d+)([mM])', value)
        if match:
            new_val = int(match.group(1)) + delta_mb
            return f'-Xmx{new_val}{match.group(2)}'
    return value

def process_xml(input_file, delta_mb, yarn_only=False):
    tree = ET.parse(input_file)
    root = tree.getroot()

    m = re.match(r'\{.*\}', root.tag)
    ns = m.group(0) if m else ''
    print(f"XML namespace detected: '{ns}'")

    modified_count = 0

    for prop in root.iter(f'{ns}property'):
        name_elem = prop.find(f'{ns}name')
        value_elem = prop.find(f'{ns}value')

        if name_elem is None or value_elem is None:
            continue

        name = name_elem.text.strip()
        value = value_elem.text.strip()

        if yarn_only:
            if name == 'yarn.app.mapreduce.am.command-opts' or name == 'yarn.app.mapreduce.am.resource.mb':
                new_value = increase_yarn_value(value, delta_mb)
                if new_value != value:
                    print(f"Modifying YARN property '{name}': '{value}' -> '{new_value}'")
                    value_elem.text = new_value
                    modified_count += 1
                else:
                    print(f"No changes required for property '{name}': '{value}'")
        else:
            if any(k in name for k in [
                'mapreduce.map.memory.mb',
                'mapreduce.map.java.opts',
                'mapreduce.reduce.memory.mb',
                'mapreduce.reduce.java.opts'
            ]):
                is_java_opt = 'java.opts' in name
                is_reduce_memory = name == 'mapreduce.reduce.memory.mb'

                new_value = increase_general_value(value, delta_mb, is_java_opt, is_reduce_memory)

                if new_value != value:
                    print(f"Modifying MR property '{name}': '{value}' -> '{new_value}'")
                    value_elem.text = new_value
                    modified_count += 1
                else:
                    print(f"No changes required for property '{name}': '{value}'")

    if modified_count > 0:
        tree.write(input_file, encoding='utf-8', xml_declaration=True)
        print(f"XML file '{input_file}' updated successfully with {modified_count} modification(s).")
    else:
        print("No properties were updated in the XML file.")

def cleanup_local_file(file_path):
    try:
        os.remove(file_path)
        print(f"Local file '{file_path}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete local file '{file_path}': {e}")

def process_oozie_workflow(coord_action_id, delta_mb, yarn_only):
    check_oozie_action_status(coord_action_id)
    print(f"\nProcessing Oozie Coordination Action ID: {coord_action_id}")
    workflow_id = extract_workflow_id(coord_action_id)
    print(f"Extracted Workflow ID: {workflow_id}")
    hadoop_path = extract_hadoop_workflow_path(workflow_id)
    print(f"Retrieved Hadoop Path: {hadoop_path}")

    #hadoop_path = "temp/gam_spray_hourly_workflow.xml_bak" (can be used for testing memory update variations)

    xml_file = download_workflow_xml(hadoop_path)
    print(f"Downloaded workflow XML file: {xml_file}")

    backup_workflow_xml(hadoop_path)
    process_xml(xml_file, delta_mb, yarn_only=yarn_only)
    upload_workflow_xml(xml_file, hadoop_path)
    cleanup_local_file(xml_file)

def main():
    parser = argparse.ArgumentParser(description="Update memory settings in Oozie workflow XML.")
    parser.add_argument("coord_action_id", help="Oozie Coord Action ID (e.g., coord-id@action-id)")
    parser.add_argument("-add", type=int, default=1, help="Number of GB (1024 MB units) to add (default: 1GB)")
    parser.add_argument("-yarn", action="store_true", help="Only increase yarn.app.mapreduce.* memory settings")

    args = parser.parse_args()
    delta_mb = args.add * 1024

    process_oozie_workflow(args.coord_action_id, delta_mb, yarn_only=args.yarn)

if __name__ == "__main__":
    main()
