import os
import multiprocessing
import numpy as np
import time

#Creates three directories
def create_dirs():
    # Check if Directories folder exists
    if not os.path.exists('./Fixed'):
        os.makedirs('./Fixed')
    if not os.path.exists('./Delimited'):
        os.makedirs('./Delimited')
    if not os.path.exists('./Offset'):
        os.makedirs('./Offset')

# Writes line to file
def write_to_file(directory, line, page_num):
    # Create file path
    file_path = f'./{directory}/page_{page_num}.txt'
    with open(file_path, 'w') as f:
        f.write(line)

# Convert values to string and left justify them with 'x' to make them 20 characters long
def conversion_for_fixed(attributes):

    # Initialize empty string
    line = ''

    # Iterate over each attribute
    for attribute in attributes:
        # ljust() left justifies the string with 'x' to make it 20 characters long
        attribute = str(attribute).ljust(20, 'x')
        
        # Append attribute to line
        line += attribute
    return line

def conversion_for_offset(attributes):
    result = ''
    current_position = 0
    
    for attr in attributes:
        result += str(attr).zfill(2) + str(current_position).zfill(2)
        current_position += 2  # Assuming a fixed offset size of 2 for each attribute
    
    return result

def extract_attributes(offset_string):
    attributes = []

    for i in range(0, len(offset_string), 4):  # Assuming each offset is of size 4 (2 for attribute and 2 for position)
        attribute = int(offset_string[i:i+2])
        attributes.append(attribute)

    return attributes

def store(input_file_path):
    # Call create_dirs() to create directories
    create_dirs()

    # Initialize variables
    page_number = 1 # Page number
    counter = 0 # Counter to keep track of number of records in a page
    fixed_content = [] # List to store 500 fixed length records
    delimited_content = [] # List to store 500 delimited records
    offset_content = [] # List to store 500 offset records
    
    # Read input file
    with open(input_file_path, 'r') as f:
        # Read lines from input file
        lines = f.readlines()

        # Iterate over lines
        for line in lines:

            # Strip line and split it by comma
            line = line.strip().split(',')

            # Check if operation is INSERT
            if line[0] == 'INSERT':

                # Convert values to int
                values = list(map(int, line[1:]))
                
                # Append values to fixed_content after converting them to specified format
                fixed_content.append(conversion_for_fixed(values))

                # Append values to delimited_content after converting them to specified format
                delimited_content.append('$'.join(map(str, values)))
                
                # Append values to offset_content after converting them to specified format
                offset_content.append(conversion_for_offset(values))

                # Increment counter
                counter += 1

                # Check if counter is 500 then write fixed_content, delimited_content and offset_content to files and reset counter and lists
                if counter == 500:
                    
                    # Write fixed_content, delimited_content and offset_content to files
                    write_to_file('Fixed', '\n'.join(fixed_content), page_number)
                    write_to_file('Delimited', '\n'.join(delimited_content), page_number)
                    write_to_file('Offset', '\n'.join(offset_content), page_number)
                    
                    # Increment page number to write to next page
                    page_number += 1

                    # Reset counter and lists
                    counter = 0
                    fixed_content = []
                    delimited_content = []
                    offset_content = []

store('input_data_files/input_1000.txt')

def analyze_fixed(page, index_of_attribute):
    if index_of_attribute > 5:
        print("Index of attribute should be between 0 and 5")
        return
    with open(f'./Fixed/{page[0]}', 'r') as f:
        values = []
        lines = f.readlines()
        for line in lines:
            value = line[index_of_attribute*20:index_of_attribute*20+20]
            values.append(int(value.rstrip('x')))
        return sum(values),len(values)

def analyze_delimited(page, index_of_attribute):
    if index_of_attribute > 5:
        print("Index of attribute should be between 0 and 5")
        return
    with open(f'./Delimited/{page[0]}', 'r') as f:
        values = []
        lines = f.readlines()
        for line in lines:
            value = line.split('$')[index_of_attribute]
            values.append(int(value))
        return sum(values),len(values)

def analyze(directory, index_of_attribute, num_processes):
    if directory not in ["Fixed", "Delimited", "Offset"]:
        print("Directory should be either Fixed, Delimited or Offset")
        return
    path = f'./{directory}'
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    files_per_process = int(np.ceil(len(files) / num_processes))
    divided_files = [files[i:i + files_per_process] for i in range(0, len(files), files_per_process)]
    pool = multiprocessing.Pool(processes=num_processes)
    if directory == "Fixed":
        results = pool.starmap(analyze_fixed, [(page, index_of_attribute) for page in divided_files])
    elif directory == "Delimited":
        results = pool.starmap(analyze_delimited, [(page, index_of_attribute) for page in divided_files])
    elif directory == "Offset":
        results = pool.starmap(analyze_delimited, [(page, index_of_attribute) for page in divided_files])
    pool.close()
    pool.join()
    average = sum([result[0] for result in results]) / sum([result[1] for result in results])
    print(f"{index_of_attribute} average: {average}")

if __name__ == '__main__':
    analyze('Fixed', 2, 8)