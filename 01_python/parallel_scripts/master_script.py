import os

# Define the folder where your Python scripts are located
scripts_folder = r"C:\Users\harshit.kumar\Desktop\ETL_Python_tasks\ETL_harshit\01_python\parallel_scripts"

# List the Python scripts in the exact order they should run
python_scripts = [
    'start_batch.py', 'oracle_to_s3.py', 's3_to_stage.py', 'stage_to_dw.py', 'end_batch.py'
]
print("Scripts to run in order:", python_scripts)

# Function to run each script using os.system()
def run_script(script):
    script_path = os.path.join(scripts_folder, script)
    command = f'python "{script_path}"'
    result = os.system(command)
    if result != 0:
        print(f"Error: Script {script} failed with exit code {result}")
    else:
        print(f"Script {script} completed successfully.")
    return result

# Function to run scripts sequentially
def run_scripts_sequentially(scripts):
    for script in scripts:
        print(f"Running {script}...")
        result = run_script(script)
        if result != 0:
            print("Stopping execution due to script failure.")
            break

if __name__ == '__main__':
    run_scripts_sequentially(python_scripts)
