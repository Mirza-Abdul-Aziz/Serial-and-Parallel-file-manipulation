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
    offset = ''
    values = ''
    current_position = 0

    for attr in attributes:
        offset += str(current_position).zfill(2)
        values += str(attr)
        current_position += 1  # Assuming a fixed offset size of 2 for each attribute
    result = offset + values
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
                if len(offset_content) == 0:
                    offset_content.append("6,2")
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

# Calculates the average of the attribute at the given index for all pages in the given directory
def analyze_fixed(page, index_of_attribute):
    # Check if index_of_attribute is between 0 and 5
    if index_of_attribute > 5:
        print("Index of attribute should be between 0 and 5")
        return
    # Read file
    with open(f'./Fixed/{page}', 'r') as f:
        values = []
        lines = f.readlines()
        # Iterate over lines
        for line in lines:
            # Extract value at given index
            value = line[index_of_attribute*20:index_of_attribute*20+20]
            # Append value to values after removing 'x' from the end
            values.append(int(value.rstrip('x')))
        # Return sum of values and length of values
        f.close()
        return sum(values),len(values)

def analyze_delimited(page, index_of_attribute):
    # Check if index_of_attribute is between 0 and 5
    if index_of_attribute > 5:
        print("Index of attribute should be between 0 and 5")
        return
    # Read file
    with open(f'./Delimited/{page}', 'r') as f:
        values = []
        lines = f.readlines()
        # Iterate over lines
        for line in lines:
            # Split line by '$' and extract value at given index
            value = line.split('$')[index_of_attribute]
            # Append value to values
            values.append(int(value))
        f.close()
        # Return sum of values and length of values
        return sum(values),len(values)

def analyze_offset(page, index_of_attribute):
    # Check if index_of_attribute is between 0 and 5
    if index_of_attribute > 5:
        print("Index of attribute should be between 0 and 5")
        return
    # Read file
    with open(f'./Offset/{page}', 'r') as f:
        values = []
        lines = f.readlines()
        # Iterate over lines
        for line in lines:
            # Extract offset string
            if line == "6,2":
                continue
            offset_string = line[11:]
            # Extract attributes from offset string
            attributes = extract_attributes(offset_string)
            print(attributes)
            # Extract value at given index
            value = attributes[index_of_attribute]
            # Append value to values
            values.append(value)
        f.close()
        # Return sum of values and length of values
        return sum(values),len(values)


def analyze(directory, index_of_attribute, num_processes):
    # Check if directory is Fixed, Delimited or Offset
    if directory not in ["Fixed", "Delimited", "Offset"]:
        print("Directory should be either Fixed, Delimited or Offset")
        return
    # Reading all files in the directory
    path = f'./{directory}'
    # Making a list of all files in the directory
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    # # Number of files per process
    # print(f"Number of files: {len(files)}")
    # files_per_process = int(np.ceil(len(files) / num_processes))
    # print(f"Files per process: {files_per_process}")
    # # Dividing files into chunks
    # divided_files = [files[i:i + files_per_process] for i in range(0, len(files), files_per_process)]
    # Creating a pool of processes
    pool = multiprocessing.Pool(processes=num_processes)
    # Calling analyze_fixed, analyze_delimited or analyze_offset based on directory
    if directory == "Fixed":
        results = pool.starmap(analyze_fixed, [(page, index_of_attribute) for page in files])
    elif directory == "Delimited":
        results = pool.starmap(analyze_delimited, [(page, index_of_attribute) for page in files])
    elif directory == "Offset":
        results = pool.starmap(analyze_delimited, [(page, index_of_attribute) for page in files])
    # Closing the pool
    pool.close()
    pool.join()
    # Calculating average from results returned by processes
    # print(results)
    average = sum([result[0] for result in results]) / sum([result[1] for result in results])
    print(f"{index_of_attribute} average: {average}")

if __name__ == '__main__':
    store('input_data_files/input_100K.txt')
    start_time = time.time()
    analyze('Fixed', 2, 8)
    print(f"Time taken for Fixed: {round((time.time() - start_time), 2)} seconds")