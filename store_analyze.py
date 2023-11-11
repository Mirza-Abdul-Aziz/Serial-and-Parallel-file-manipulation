import sys,os
import multiprocessing
import time

#Creates three directories
def create_dirs():

    # Check if Directories folder exists
    if not os.path.exists('./Directories/Fixed'):
        os.makedirs('./Directories/Fixed')
    if not os.path.exists('./Directories/Delimited'):
        os.makedirs('./Directories/Delimited')
    if not os.path.exists('./Directories/Offset'):
        os.makedirs('./Directories/Offset')

# Writes line to file
def write_to_file(directory, line, page_num):
    

    file_path = f'./Directories/{directory}/page_{page_num}.txt'
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

def conversion_for_offset(values):
    return ' '.join(map(str, values))

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
                
                # Increment counter
                counter += 1

                # Check if counter is 500 then write fixed_content, delimited_content and offset_content to files and reset counter and lists
                if counter == 500:
                    
                    # Write fixed_content, delimited_content and offset_content to files
                    write_to_file('Fixed', '\n'.join(fixed_content), page_number)
                    write_to_file('Delimited', '\n'.join(delimited_content), page_number)
                    
                    # Increment page number to write to next page
                    page_number += 1

                    # Reset counter and lists
                    counter = 0
                    fixed_content = []
                    delimited_content = []
                    offset_content = []

store('./input_data_files/input_1000.txt')                

# if __name__ == '__main__':