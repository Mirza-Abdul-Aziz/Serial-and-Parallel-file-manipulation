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
    file_path = f'./{directory}/page_{page_num}.txt'
    with open(file_path, 'a') as f:
        f.write(line)

def store(input_file_path):
    create_dirs()
    page_number = 1
    counter = 1
    with open(input_file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            #print(line)
            if line[0] == 'INSERT':
                write_to_file('Fixed', line, page_number)
                write_to_file('Delimited', line, page_number)
                write_to_file('Offset', line, page_number)
                counter += 1 
                if counter == 500:
                    page_number += 1
                    counter = 1
                

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


