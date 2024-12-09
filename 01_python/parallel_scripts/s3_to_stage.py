import os
from concurrent.futures import ThreadPoolExecutor

# Define the folder where your Python scripts are located
scripts_folder = r"C:\Users\harshit.kumar\Desktop\ETL_Python_tasks\ETL_harshit\01_python\s3_to_stage_scripts"

# Get all Python scripts in the folder
python_scripts = ['customers.py', 'employees.py', 'offices.py', 'orderdetails.py', 'orders.py', 'payments.py', 'productlines.py', 'products.py']
print(python_scripts)

# Function to run each script using os.system()
def run_script(script):
    script_path = os.path.join(scripts_folder, script)
    command = f'python "{script_path}"'
    result = os.system(command)
    return f"Script {script} finished with exit code {result}"

# Function to run scripts in parallel
def run_scripts_in_parallel(scripts):
    with ThreadPoolExecutor() as executor:
        results = executor.map(run_script, scripts)
        for result in results:
            print(result)

if __name__ == '__main__':
    run_scripts_in_parallel(python_scripts)






### Method - 2

# from multiprocessing import Process


# # Define the folder where your Python scripts are located
# scripts_folder = r"C:\Users\nikhil.pabbithi\Downloads\ETL Python"

# # Get all Python scripts in the folder
# python_scripts = [f for f in os.listdir(scripts_folder) if f.endswith('.py')]

# # Function to run each script
# def run_script(script):
#     script_path = os.path.join(scripts_folder, script)
#     os.system(f'python "{script_path}"')

# # Function to run scripts in parallel using multiprocessing
# def run_scripts_in_parallel(scripts):
#     processes = []
#     for script in scripts:
#         process = Process(target=run_script, args=(script,))
#         processes.append(process)
#         process.start()

#     for process in processes:
#         process.join()

# if __name__ == '__main__':
#     run_scripts_in_parallel(python_scripts)
