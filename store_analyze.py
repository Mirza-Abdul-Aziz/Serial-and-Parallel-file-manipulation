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

#Creates three files
create_dirs()
# def store():
    
# def analyze():



# if __name__ == '__main__':


