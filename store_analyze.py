import sys,os
import multiprocessing
import time

#Creates three directories
def create_dirs():
    if not os.path.exists('./Directories/Fixed'):
        os.makedirs('./Directories/Fixed')
    if not os.path.exists('./Directories/Delimited'):
        os.makedirs('./Directories/Delimited')
    if not os.path.exists('./Directories/Offset'):
        os.makedirs('./Directories/Offset')

def write_to_file(directory, line, page_num):
    file_path = f'./Directories/{directory}/page_{page_num}.txt'
    with open(file_path, 'w') as f:
        f.write(line)
def conversion_for_fixed(values):
    line = ''
    for value in values:
        value = str(value).ljust(20, 'x')
        line += value
    return line
def conversion_for_delimited(values):
    return '$'.join(map(str, values))
def conversion_for_offset(values):
    return ' '.join(map(str, values))
def store(input_file_path):
    create_dirs()
    page_number = 1
    counter = 0
    fixed_content = []
    delimited_content = []
    offset_content = []
    with open(input_file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip().split(',')
            if line[0] == 'INSERT':
                values = list(map(int, line[1:]))
                fixed_content.append(conversion_for_fixed(values))
                delimited_content.append('$'.join(map(str, values)))
                counter += 1 
                print(counter)
                if counter == 500:
                    write_to_file('Fixed', '\n'.join(fixed_content), page_number)
                    write_to_file('Delimited', '\n'.join(delimited_content), page_number)
                    page_number += 1
                    counter = 0
                    fixed_content = []
                    delimited_content = []

store('./input_data_files/input_1000.txt')                

            #     with open('./Directories/Fixed/fixed.txt', 'a') as fixed:
            #         fixed.write(line)
            # elif line[0] == 'D':
            #     with open('./Directories/Delimited/delimited.txt', 'a') as delimited:
            #         delimited.write(line)
            # elif line[0] == 'O':
            #     with open('./Directories/Offset/offset.txt', 'a') as offset:
            #         offset.write(line)
            # else:
            #     print("Error: Invalid line in input file.
    
# def analyze():



# if __name__ == '__main__':


