#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 23:45:35 2020

@author: yaroslav
"""
import manticore_tools as tools

MAX_EVENT_NUMBER = 0
MIN_EVENT_NUMBER = 999999999999
TOTAL_DICT_OF_DAYS = {}
# =============================================================================
#
# =============================================================================
def set_of_tails(files_list, day):
    """Creates tails_set for every day. Works for every day from days_set.

    Tails_set contains the tails of all files preprocessed in the
    directory of this day. For example, if you preprocessed only the
    files xxx.001, set will contain only one item - "001". If you
    preprocessed all the day, the set will contain all the tails from
    ('001', '002', '003', ..., the last one) of this day."""

    global TOTAL_DICT_OF_DAYS
    tails_set = set()
    for file in files_list:
        if file[:-19] == day:
            file = tools.check_and_cut_the_tail(file)
            tails_set.add(file[60:])
    print("Set of tails have been created.")
    return tails_set

def set_of_days(files_list):
    """Creates days_set which contain full pathes of all the days present
    in .files_list. If you preprocessed one day, there will be only it."""

    days_set = set()
    for file in files_list:
        file = tools.check_and_cut_the_tail(file)
        file_directory = file[:-18]
        days_set.add(file_directory)
    print("Set of days have been created.")
    return days_set

def create_empty_dict_of_days(start_time):
    
    global MIN_EVENT_NUMBER
    global MAX_EVENT_NUMBER
    
    dict_of_max_min = {}
    print("Event numbers range in parallel bsms are finding out...")
    with open('.files_list.txt', 'r') as files:
        files_list = files.readlines()
    days_list = sorted(list(set_of_days(files_list)))

    for day in days_list:
        tails_list = sorted(list(set_of_tails(files_list, day)))
        for tail in tails_list:
            dict_of_max_min[tail] = [MIN_EVENT_NUMBER, MAX_EVENT_NUMBER]
            TOTAL_DICT_OF_DAYS[day] = dict_of_max_min
# =============================================================================
#
# =============================================================================

def to_process(start_time):
    """Manages the conveyor of processing. Put all the files on it in order.

    Takes .files_list.txt and file by file (line by line) put them to the
    process_one_file function like children in "Another Brick In The Wall".
    In addition provides all needed interface. Exactly from here
    comes the BASH outstream through all binary files cleaning."""

    create_empty_dict_of_days(start_time)
    with open('.files_list.txt', 'r') as file_of_files:
        number_of_files_to_process = len(file_of_files.readlines())
    with open(".files_list.txt", "r") as files_list:
        print("\nStart to process...\n")
        counter = 0
        for file_to_process in files_list:
            file_to_process = tools.check_and_cut_the_tail(file_to_process)            
            tail = file_to_process[-3:]
            file_day = file_to_process[:-18]
            
            min_number, max_number = to_process_single_file(file_to_process)
            if min_number != "empty" and max_number != "empty":
                        
                if min_number < TOTAL_DICT_OF_DAYS[file_day][tail][0]:
                    TOTAL_DICT_OF_DAYS[file_day][tail][0] = min_number
                if max_number > TOTAL_DICT_OF_DAYS[file_day][tail][1]:
                    TOTAL_DICT_OF_DAYS[file_day][tail][1] = max_number
            
            print("\nPreparing binary files:\n")
            counter += 1
            tools.syprogressbar(
                counter,
                number_of_files_to_process,
                u'\u24B7',
                "preparing binary files",
                start_time)
            print("\n{} is processing now.".format(file_to_process))
            
    with open(".total_dict_of_days.txt", "w+") as dict_file:
        for day, tail_list in TOTAL_DICT_OF_DAYS.items():
            dict_file.write(str(day) + "\n")
            dict_file.write(str(len(tail_list)) + "\n")
            for tail, list_of_max_min in tail_list.items():
                dict_file.write("{}\t{}\t{}\n".format(tail, list_of_max_min[0], list_of_max_min[1]))
# =============================================================================
#
# =============================================================================
'''
def to_process_single_file_without_existance_checking(file_to_process):
    """Conducts one input file through all sequence of needed operations.

    Firstly checks is needed pedestal files exist. If no:
    - calculates pedestals,
    - calculates pedestals sigmas,
    - make ignore file, which contains information is channel make noise
    (concluded from the sigmas).
    If needed pedestal files exists, skips it and calculates amplitudes
    for all events in this files and lightweight header file. By the way,
    header file is not further used anywhere. It is rather a tribute to
    tradition. What the data processing without header files?!

    The details of functions see in their own docstrings."""

    with open(".mess.txt", "a") as mess_file:

        make_quick_pedestals(file_to_process)
#           .qpd - quick pedestals
        print("Cleaning for pedestals")
        mess_file.write("Made temporary file:  {}.qpd\n".format(
                tools.make_PED_file_temp(file_to_process)))
        print("Made temporary file:  {}.fpd".format(
                tools.make_PED_file_temp(file_to_process)))
#           .sgm - sigmas of pedestals
        print("Calculating pedestals sigmas...")
        mess_file.write("Made temporary file:  {}.sgm\n".format(
                tools.make_PED_file_temp(file_to_process)))
        print("Made temporary file:  {}.sgm".format(
                tools.make_PED_file_temp(file_to_process)))
#           .ig ignore status from sigma
#           (PED_sigma < PED_average + 3*sigma_sigma)
        print("Compiling file with channels to be ignored...")
        mess_file.write("Made temporary file:  {}.ig\n".format(
                tools.make_PED_file_temp(file_to_process)))
        print("Made temporary file:  {}.ig".format(
                tools.make_PED_file_temp(file_to_process)))

        min_number, max_number = make_clean_amplitudes_and_headers(file_to_process)
#           .hdr - file with only events number #1, event number #2,
#           time of event and maroc number
        print("Creating of the header...")
        mess_file.write("Made temporary file:  {}.hdr\n".format(
                tools.make_BSM_file_temp(file_to_process)))
        print("Made temporary file:  {}.hdr".format(
                tools.make_BSM_file_temp(file_to_process)))
#           .wfp - amplitudes minus fine pedestals
        print("Cleaning for pedestals...")
        mess_file.write("Made temporary file:  {}.wqp\n".format(
                tools.make_BSM_file_temp(file_to_process)))
        print("Made temporary file:  {}.wqp".format(
                tools.make_BSM_file_temp(file_to_process)))
    return min_number, max_number
'''
#==============================================================================
#
# =============================================================================
def to_process_single_file(file_to_process):
    """Conducts one input file through all sequence of needed operations.

    Firstly checks is needed pedestal files exist. If no:
    - calculates pedestals,
    - calculates pedestals sigmas,
    - make ignore file, which contains information is channel make noise
    (concluded from the sigmas).
    If needed pedestal files exists, skips it and calculates amplitudes
    for all events in this files and lightweight header file. By the way,
    header file is not further used anywhere. It is rather a tribute to
    tradition. What the data processing without header files?!

    The details of functions see in their own docstrings."""

    with open(".mess.txt", "a") as mess_file:
        
        if (tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".spd") or
                tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".ssg") or
                tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".sig")) is False:
#           .spd - static pedestals
            print("Static pedestals files creating...")
            mess_file.write("Made temporary file:  {}.spd\n".format(
                    tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.spd".format(
                    tools.make_PED_file_temp(file_to_process)))
#           .ssg - sigmas of static pedestals
            print("Calculating static pedestals sigmas...")
            mess_file.write("Made temporary file:  {}.ssg\n".format(
                    tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.ssg".format(
                    tools.make_PED_file_temp(file_to_process)))
#           .sig ignore status from static pedestals sigma
#           (PED_sigma < PED_average + 3*sigma_sigma)
            print("Compiling file with channels to be ignored...")
            mess_file.write("Made temporary file:  {}.sig\n".format(
                    tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.sig".format(
                    tools.make_PED_file_temp(file_to_process)))
            make_static_pedestals(file_to_process)
        else: print("Static pedestal file exists.")
        
        if (tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".dpd") or
                tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".dsg") or
                tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".dig")) is False:
#           .dpd - dynamic pedestals
            print("Dynamic pedestals files creating...")
            mess_file.write("Made temporary file:  {}.dpd\n".format(
                    tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.dpd".format(
                    tools.make_BSM_file_temp(file_to_process)))
#           .dsg - sigmas of dynamic pedestals
            print("Calculating dynamic pedestals sigmas...")
            mess_file.write("Made temporary file:  {}.dsg\n".format(
                    tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.dsg".format(
                    tools.make_BSM_file_temp(file_to_process)))
#           .dig ignore status from dynamic pedestals sigma
#           (PED_sigma < PED_average + 3*sigma_sigma)
            print("Compiling file with channels to be ignored...")
            mess_file.write("Made temporary file:  {}.dig\n".format(
                    tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.dig".format(
                    tools.make_BSM_file_temp(file_to_process)))
            make_dynamic_pedestals(file_to_process)
        else: print("Dynamic pedestal file exists.")
        
        if (tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".amp") or
            tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".hdr")) is False:
#           .hdr - file with only events number #1, event number #2,
#           time of event and maroc number
            print("Creating of the header...")
            mess_file.write("Made temporary file:  {}.hdr\n".format(
                   tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.hdr".format(
                   tools.make_BSM_file_temp(file_to_process)))
#           .amp - raw amplitudes
            print("The raw amplitudes file creating...")
            mess_file.write("Made temporary file:  {}.amp\n".format(
                    tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.amp".format(
                    tools.make_BSM_file_temp(file_to_process)))
            min_number, max_number = make_clean_amplitudes_and_headers_1(file_to_process)
        else:
            print("Raw amplitude file is exist.")
            min_number, max_number = "empty", "empty"
        
        if (tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".asp")) is False:
#           .asp - amplitudes minus static pedestals
            print("The static amplitudes file creating...")
            mess_file.write("Made temporary file:  {}.asp\n".format(
                   tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.asp".format(
                   tools.make_BSM_file_temp(file_to_process)))
            make_static_amplitudes(file_to_process)
        else: print("STatic amplitude file is exist.")
        
        if (tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".adp")) is False:
#           .adp - amplitudes minus dynamic pedestals
            print("The raw amplitudes file creating...")
            mess_file.write("Made temporary file:  {}.adp\n".format(
                    tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.adp".format(
                    tools.make_BSM_file_temp(file_to_process)))
            make_dynamic_amplitudes(file_to_process)
        else: print("Dynamic amplitude file is exist.")
    return min_number, max_number
#==============================================================================
#
# =============================================================================
'''
def to_process_single_file_with_static_dynamic_features(file_to_process):
    """Conducts one input file through all sequence of needed operations.

    Firstly checks is needed pedestal files exist. If no:
    - calculates pedestals,
    - calculates pedestals sigmas,
    - make ignore file, which contains information is channel make noise
    (concluded from the sigmas).
    If needed pedestal files exists, skips it and calculates amplitudes
    for all events in this files and lightweight header file. By the way,
    header file is not further used anywhere. It is rather a tribute to
    tradition. What the data processing without header files?!

    The details of functions see in their own docstrings."""

    with open(".mess.txt", "a") as mess_file:
        if (tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".spd") or
                tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".ssg") or
                tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".sig")) is False:
            make_static_amplitudes(file_to_process)
#           .spd - static pedestals
            print("Static pedestals file creating...")
            mess_file.write("Made temporary file:  {}.spd\n".format(
                tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.fpd".format(
                tools.make_PED_file_temp(file_to_process)))
#           .sgm - sigmas of pedestals
            print("Calculating pedestals sigmas...")
            mess_file.write("Made temporary file:  {}.sgm\n".format(
                tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.sgm".format(
                tools.make_PED_file_temp(file_to_process)))
#           .ig ignore status from sigma
#           (PED_sigma < PED_average + 3*sigma_sigma)
            print("Compiling file with channels to be ignored...")
            mess_file.write("Made temporary file:  {}.ig\n".format(
                tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.ig".format(
                tools.make_PED_file_temp(file_to_process)))
        else: print("Pedestal file exists.")
        if (tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".wqp") or
            tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".hdr")) is False:
            min_number, max_number = make_clean_amplitudes_and_headers(file_to_process)
#           .hdr - file with only events number #1, event number #2,
#           time of event and maroc number
            print("Creating of the header...")
            mess_file.write("Made temporary file:  {}.hdr\n".format(
                tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.hdr".format(
                tools.make_BSM_file_temp(file_to_process)))
#           .wfp - amplitudes minus fine pedestals
            print("Cleaning for pedestals...")
            mess_file.write("Made temporary file:  {}.wqp\n".format(
                tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.wqp".format(
                tools.make_BSM_file_temp(file_to_process)))
        else: print("Cleaned files exists.")
    return min_number, max_number
'''
#==============================================================================
#
# =============================================================================
'''
def make_quick_pedestals(file_to_process):

    with open(file_to_process[:-18] + "PED/" + file_to_process[-12:-4] + ".ped", "rb") as ped_fin:

        counter = [0]*64
        PED_square = [0]*64
        PED = [0]*64
        PED_av = [0]*64
        PED_sigma = [0]*64
        ignore_status = [0]*64
        sigma_sigma = 0
        chunk_size = 156
        codes_beginning_byte = 24
        codes_ending_byte = 152
        number_of_codes = 64
        chunk = ped_fin.read(chunk_size)
        chunk_counter = 0
        while chunk:
            try:
                codes_array = tools.unpacked_from_bytes(
                    '<64h',
                chunk[codes_beginning_byte:codes_ending_byte])
                cycle_ampl_matrix = [0]*64
                for i in range(number_of_codes):
                    cycle_ampl_matrix[i] = codes_array[i]/4
                for i in range(len(cycle_ampl_matrix)):
                    PED[i] += cycle_ampl_matrix[i]
                    PED_square[i] += cycle_ampl_matrix[i]*cycle_ampl_matrix[i]
                    counter[i] += 1
            except Exception:
                print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                    "RAW CHUNK CORRUPTION ERROR!",
                    chunk_counter,
                    file_to_process[:-18] + "PED/" + file_to_process[-12:-4] + ".ped"))
            chunk_counter += 1
            chunk = ped_fin.read(chunk_size)

    for i in range(len(PED)):
        PED_av[i] = PED[i]/counter[i]
    for i in range(len(PED)):
        PED_square[i] = PED_square[i] / (counter[i] - 1)
#   Deviation from real SIGMA is less then 0.5%
    for i in range(len(PED)):
        PED_sigma[i] = tools.square_root(
            abs(PED_square[i] - (PED_av[i]*PED_av[i]*counter[i])/(counter[i] - 1)))
    with open(tools.make_PED_file_temp(file_to_process) +".qpd", "wb") as ped_fout:
        print(tools.make_PED_file_temp(file_to_process) +".qpd")
        peds_average = tools.packed_bytes('<64f', PED_av)
        ped_fout.write(peds_average)
    with open(tools.make_PED_file_temp(file_to_process) +".sgm", "wb") as sigma_fout:
        peds_sigma = tools.packed_bytes('<64f', PED_sigma)
        sigma_fout.write(peds_sigma)
    sigma_av = sum(PED_sigma)/len(PED_sigma)
    for item in PED_sigma:
        sigma_sigma += (sigma_av - item)**2
    sigma_sigma = tools.square_root(sigma_sigma/len(PED_sigma))
# BOTH are inside 3-sigma
    for i in range(0, len(PED), 2):
        if (abs(PED_sigma[i]) < abs(PED_av[i] + 3*sigma_sigma) and abs(PED_sigma[i+1]) < abs(PED_av[i+1] + 3*sigma_sigma)):
            ignore_status[i] = 0
# BOTH are outside 3-sigma
        if (abs(PED_sigma[i]) > abs(PED_av[i] + 3*sigma_sigma) and abs(PED_sigma[i+1]) < abs(PED_av[i+1] + 3*sigma_sigma)):
            ignore_status[i] = 1
# BOTH are outside 3-sigma
        if (abs(PED_sigma[i]) < abs(PED_av[i] + 3*sigma_sigma) and abs(PED_sigma[i+1]) > abs(PED_av[i+1] + 3*sigma_sigma)):
            ignore_status[i] = 2
# BOTH are outside 3-sigma
        if (abs(PED_sigma[i]) > abs(PED_av[i] + 3*sigma_sigma) and abs(PED_sigma[i+1]) > abs(PED_av[i+1] + 3*sigma_sigma)):
            ignore_status[i] = 3
# BOTH are outside 3-sigma
        else: ignore_status[i] = 4

    with open(tools.make_PED_file_temp(file_to_process) +".ig", "wb") as ignore_status_fout:
        ignore_pack = tools.packed_bytes('<64B', ignore_status)
        ignore_status_fout.write(ignore_pack)
'''
# =============================================================================
#
# =============================================================================
'''
def make_clean_amplitudes_and_headers(file_to_process):

    chunk_size = 156
    codes_beginning_byte = 24
    codes_ending_byte = 152
    number_1_beginning_byte = 4
    number_1_ending_byte = 8
    maroc_number_byte = 20

    min_number = 999999999999
    max_number = 0

    with open(tools.make_PED_file_temp(file_to_process) + ".qpd", "rb") as ped_fin:
        peds = ped_fin.read()
        try:
            peds_array = tools.unpacked_from_bytes('<64f', peds)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "QPD-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".qpd"))

    with open(tools.make_PED_file_temp(file_to_process) + ".ig", "rb") as ig_fin:
        ig_bytes = ig_fin.read()
        try:
            ig_array = tools.unpacked_from_bytes('<64B', ig_bytes)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "IG-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".ig"))

    with open(file_to_process, "rb") as codes_fin:
        with open(tools.make_BSM_file_temp(file_to_process) + ".wqp", "wb") as cleaned_file:
            with open(tools.make_BSM_file_temp(file_to_process) + ".hdr", "wb") as header_file:

                chunk_counter = 0
                chunk = codes_fin.read(chunk_size)
                while chunk:
                    try:
                        cleaned_amplitudes = [0]*96
                        event_number = tools.unpacked_from_bytes('I', chunk[number_1_beginning_byte:number_1_ending_byte])[0]
                        if event_number > max_number:
                            max_number = event_number
                        if event_number < min_number:
                            min_number = event_number
                        codes_array = tools.unpacked_from_bytes(
                            '<64h', chunk[codes_beginning_byte:codes_ending_byte])
                        for i in range(0, len(cleaned_amplitudes), 3):
                            if codes_array[2*i//3] <= 1800:
                                cleaned_amplitudes[i] =\
                                codes_array[2*i//3]/4 - peds_array[2*i//3]
                            else:
                                cleaned_amplitudes[i] =\
                                codes_array[2*i//3 + 1]/4 - peds_array[2*i//3 + 1]
                            cleaned_amplitudes[i+1] =\
                            int(bin(codes_array[2*i//3 + 1])[-1])
#                           ig_status = 0 if BOTH channels IS ignored
#                           1 if LOW IS NOT ignored, HIGH IS ignored
#                           2 if HIGH IS NOT ignored, LOW IS ignored
#                           3 if BOTH IS NOT ignored
                            cleaned_amplitudes[i+2] =\
                            ig_array[2*i//3] + ig_array[2*i//3 + 1]
                        cleaned_amplitudes_pack = tools.packed_bytes(
                            'fBB'*32,
                            cleaned_amplitudes)
                        # chunk_size in wfp_file will be 282 bytes
                        cleaned_file.write(chunk[:codes_beginning_byte])
                        cleaned_file.write(cleaned_amplitudes_pack)
                        cleaned_file.write(chunk[codes_ending_byte:])
                        # chunk_size in header will be 17 bytes
                        header_file.write(
                            chunk[number_1_beginning_byte:maroc_number_byte +1])
                    except Exception:
                        print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                            "RAW CHUNK CORRUPTION ERROR!",
                            chunk_counter,
                            file_to_process))
                    chunk_counter += 1
                    chunk = codes_fin.read(chunk_size)
    return min_number, max_number
'''
# =============================================================================
#
# =============================================================================

def make_static_pedestals (file_to_process):
    """This function is the attempt to rebuild the original function
    'make_quick_pedestal' in more accurate way."""
    
    counter = 0 
    number_of_codes = 64 
    PED = [] 
    PED_av = [0]*number_of_codes 
    PED_sum = [0]*number_of_codes 
    PED_sigma = [0]*number_of_codes 
    sigma_sigma = 0 
    ignore_status = [0]*64 
    chunk_size = 156 
    codes_beginning_byte = 24 
    codes_ending_byte = 152 
    
    with open(file_to_process[:-18] + "PED/" + file_to_process[-12:-4] + ".ped", "rb") as ped_fin: 
        chunk = ped_fin.read(chunk_size) 
        while chunk: 
            ped_array = list(tools.unpacked_from_bytes("<64h", chunk[codes_beginning_byte:codes_ending_byte])) 
            for i in range(number_of_codes): 
                ped_array[i] /= 4 
            PED.append(ped_array) 
            for i in range(number_of_codes): 
                PED_av[i] += ped_array[i] 
            counter += 1 
            chunk = ped_fin.read(chunk_size) 
            
    for i in range(number_of_codes): 
        PED_av[i] /= counter 
    for line in PED: 
        for i in range(len(line)): 
            line[i] = tools.square_root((line[i] - PED_av[i])*(line[i] - PED_av[i])) 
    for line in PED: 
        for i in range(len(line)): 
            PED_sum[i] += line[i] 
    for i in range(number_of_codes): 
        PED_sigma[i] = PED_sum[i]/counter 
    sigma_av = sum(PED_sigma)/len(PED_sigma) 
    for item in PED_sigma: 
        sigma_sigma += (sigma_av - item)**2 
    sigma_sigma = tools.square_root(sigma_sigma/len(PED_sigma)) 
    for i in range(len(PED_av)): 
        if not (-1*sigma_av - 3*sigma_sigma < PED_sigma[i] < sigma_av + 3*sigma_sigma): 
            ignore_status[i] += (i%2 +1) 
    with open(tools.make_PED_file_temp(file_to_process) + ".sig", "wb") as ignore_status_fout: 
        ignore_pack = tools.packed_bytes('<64B', ignore_status) 
        ignore_status_fout.write(ignore_pack) 
    with open(tools.make_PED_file_temp(file_to_process) + ".spd", "wb") as ped_fout: 
        peds_average = tools.packed_bytes('<64f', PED_av) 
        ped_fout.write(peds_average) 
    with open(tools.make_PED_file_temp(file_to_process) + ".ssg", "wb") as sigma_fout: 
        peds_sigma = tools.packed_bytes('<64f', PED_sigma) 
        sigma_fout.write(peds_sigma) 

# =============================================================================
#
# =============================================================================
def make_dynamic_pedestals(file_to_process):
    counter = [0]*64
    number_of_codes = 64 
    PED = [] 
    PED_av = [0]*number_of_codes 
    PED_sum = [0]*number_of_codes 
    PED_sigma = [0]*number_of_codes 
    sigma_sigma = 0 
    ignore_status = [0]*64 
    chunk_size = 156 
    codes_beginning_byte = 24 
    codes_ending_byte = 152 

    with open(file_to_process, "rb") as codes_fin:
        chunk = codes_fin.read(chunk_size) 
        while chunk: 
            codes_array = list(tools.unpacked_from_bytes("<64h", chunk[codes_beginning_byte:codes_ending_byte]))
            for i in range(0, len(codes_array), 2):
                trigger_indicator = int(bin(codes_array[i])[-1])
                if trigger_indicator == 1:
                    codes_array[i] = 0
                    codes_array[i+1] = 0
                else:
                    counter[i] += 1
                    counter[i+1] += 1
            for i in range(number_of_codes): 
                if codes_array[i] != 0:
                    codes_array[i] /= 4 
            PED.append(codes_array) 
            for i in range(number_of_codes): 
                PED_av[i] += codes_array[i] 
            chunk = codes_fin.read(chunk_size)
            
    for i in range(number_of_codes):
        if counter[i] != 0:
            PED_av[i] /= counter[i]
    for line in PED: 
        for i in range(len(line)): 
            line[i] = tools.square_root((line[i] - PED_av[i])*(line[i] - PED_av[i])) 
    for line in PED: 
        for i in range(len(line)): 
            PED_sum[i] += line[i] 
    for i in range(number_of_codes): 
        PED_sigma[i] = PED_sum[i]/counter[i] 
    sigma_av = sum(PED_sigma)/len(PED_sigma) 
    for item in PED_sigma: 
        sigma_sigma += (sigma_av - item)**2 
    sigma_sigma = tools.square_root(sigma_sigma/len(PED_sigma)) 
    for i in range(len(PED_av)): 
        if not (-1*sigma_av - 3*sigma_sigma < PED_sigma[i] < sigma_av + 3*sigma_sigma): 
            ignore_status[i] += (i%2 +1) 
    with open(tools.make_BSM_file_temp(file_to_process) + ".dig", "wb") as ignore_status_fout: 
        ignore_pack = tools.packed_bytes('<64B', ignore_status) 
        ignore_status_fout.write(ignore_pack) 
    with open(tools.make_BSM_file_temp(file_to_process) + ".dpd", "wb") as ped_fout: 
        peds_average = tools.packed_bytes('<64f', PED_av) 
        ped_fout.write(peds_average) 
    with open(tools.make_BSM_file_temp(file_to_process) + ".dsg", "wb") as sigma_fout: 
        peds_sigma = tools.packed_bytes('<64f', PED_sigma) 
        sigma_fout.write(peds_sigma) 

# =============================================================================
#
# =============================================================================
def make_clean_amplitudes_and_headers_1 (file_to_process):
    """This function is the attempt to rebuild the original function
    'make_clean_amplitudes_and_headers' in more accurate way."""

    chunk_size = 156
    codes_beginning_byte = 24
    codes_ending_byte = 152
    number_1_beginning_byte = 4
    number_1_ending_byte = 8
    maroc_number_byte = 20

    min_number = 999999999999
    max_number = 0

    with open(file_to_process, "rb") as codes_fin:
        with open(tools.make_BSM_file_temp(file_to_process) + ".amp", "wb") as clean_file:
            with open(tools.make_BSM_file_temp(file_to_process) + ".hdr", "wb") as header_file:

                chunk_counter = 0
                chunk = codes_fin.read(chunk_size)
                while chunk:
#                    try:
                    clean_amplitudes = [0]*64
                    event_number = tools.unpacked_from_bytes('I', chunk[number_1_beginning_byte:number_1_ending_byte])[0]
                    if event_number > max_number:
                        max_number = event_number
                    if event_number < min_number:
                        min_number = event_number
                    codes_array = tools.unpacked_from_bytes(
                        '<64h', chunk[codes_beginning_byte:codes_ending_byte])
                    for i in range(0, len(clean_amplitudes), 2):
                        if codes_array[i] <= 1800:
                            clean_amplitudes[i] = codes_array[i]/4
                            clean_amplitudes[i+1] = 0
                        else:
                            clean_amplitudes[i] = codes_array[i + 1]/4
                            clean_amplitudes[i+1] = 1
#                        clean_amplitudes[i+1] = int(bin(codes_array[i])[-1])
                        
                    clean_amplitudes_pack = tools.packed_bytes(
                        'fB'*32,
                        clean_amplitudes)
                        # chunk_size in wfp_file will be 282 bytes
                    clean_file.write(chunk[:codes_beginning_byte])
                    clean_file.write(clean_amplitudes_pack)
                    clean_file.write(chunk[codes_ending_byte:])
                        # chunk_size in header will be 17 bytes
                    header_file.write(
                        chunk[number_1_beginning_byte:maroc_number_byte +1])
#                    except Exception:
#                        print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
#                            "RAW CHUNK CORRUPTION ERROR!",
#                            chunk_counter,
#                            file_to_process))
                    chunk_counter += 1
                    chunk = codes_fin.read(chunk_size)
    return min_number, max_number    

# =============================================================================
#
# =============================================================================
def make_static_amplitudes(file_to_process):
    
    chunk_size = 156
    codes_beginning_byte = 24
    codes_ending_byte = -4
#    number_1_beginning_byte = 4
#    number_1_ending_byte = 8
#    maroc_number_byte = 20
    
    with open(tools.make_PED_file_temp(file_to_process) + ".spd", "rb") as ped_fin:
        peds = ped_fin.read()
        try:
            peds_array = tools.unpacked_from_bytes('<64f', peds)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "QPD-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".spd"))

    with open(tools.make_PED_file_temp(file_to_process) + ".sig", "rb") as ig_fin:
        ig_bytes = ig_fin.read()
        try:
            ig_array = tools.unpacked_from_bytes('<64B', ig_bytes)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "IG-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".sig"))

    with open(file_to_process, "rb") as codes_fin:
        with open(tools.make_BSM_file_temp(file_to_process) + ".asp", "wb") as cleaned_file:

            chunk_counter = 0
            chunk = codes_fin.read(chunk_size)
            while chunk:
#                try:
                cleaned_amplitudes = [0]*96
#                        event_number = tools.unpacked_from_bytes('I', chunk[number_1_beginning_byte:number_1_ending_byte])[0]
#                        if event_number > max_number:
#                            max_number = event_number
#                        if event_number < min_number:
#                            min_number = event_number
                codes_array = tools.unpacked_from_bytes(
                    '<64h', chunk[codes_beginning_byte:codes_ending_byte])
                for i in range(0, len(cleaned_amplitudes), 3):
                    if codes_array[2*(i//3)] <= 1800:
                        cleaned_amplitudes[i] =\
                        codes_array[2*(i//3)]/4 - peds_array[2*(i//3)]
                        cleaned_amplitudes[i+1] = 0
                    else:
                        cleaned_amplitudes[i] =\
                        codes_array[2*(i//3) + 1]/4 - peds_array[2*(i//3) + 1]
                        cleaned_amplitudes[i+1] = 1
#                    cleaned_amplitudes[i+1] = int(bin(codes_array[2*(i//3)])[-1])
#                           ig_status = 0 if BOTH channels IS ignored
#                           1 if LOW IS NOT ignored, HIGH IS ignored
#                           2 if HIGH IS NOT ignored, LOW IS ignored
#                           3 if BOTH IS NOT ignored
                    cleaned_amplitudes[i+2] =\
                    ig_array[2*(i//3)] + ig_array[2*(i//3) + 1]
                cleaned_amplitudes_pack = tools.packed_bytes(
                    'fBB'*32,
                    cleaned_amplitudes)
                        # chunk_size in wfp_file will be 282 bytes
                cleaned_file.write(chunk[:codes_beginning_byte])
                cleaned_file.write(cleaned_amplitudes_pack)
                cleaned_file.write(chunk[codes_ending_byte:])
                        # chunk_size in header will be 17 bytes
#            except Exception:
#                print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
#                    "RAW CHUNK CORRUPTION ERROR!",
#                    chunk_counter,
#                    file_to_process))
                chunk_counter += 1
                chunk = codes_fin.read(chunk_size)
#    return min_number, max_number    


# =============================================================================
#
# =============================================================================
def make_dynamic_amplitudes(file_to_process):
    
    chunk_size = 156
    codes_beginning_byte = 24
    codes_ending_byte = 152
#    number_1_beginning_byte = 4
#    number_1_ending_byte = 8
#    maroc_number_byte = 20
    
    with open(tools.make_BSM_file_temp(file_to_process) + ".dpd", "rb") as ped_fin:
        peds = ped_fin.read()
        try:
            peds_array = tools.unpacked_from_bytes('<64f', peds)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "QPD-file CORRUPTION ERROR!",
                tools.make_BSM_file_temp(file_to_process) + ".dpd"))

    with open(tools.make_BSM_file_temp(file_to_process) + ".dig", "rb") as ig_fin:
        ig_bytes = ig_fin.read()
        try:
            ig_array = tools.unpacked_from_bytes('<64B', ig_bytes)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "IG-file CORRUPTION ERROR!",
                tools.make_BSM_file_temp(file_to_process) + ".dig"))

    with open(file_to_process, "rb") as codes_fin:
        with open(tools.make_BSM_file_temp(file_to_process) + ".adp", "wb") as cleaned_file:

            chunk_counter = 0
            chunk = codes_fin.read(chunk_size)
            while chunk:
#                try:
                cleaned_amplitudes = [0]*96
#                        event_number = tools.unpacked_from_bytes('I', chunk[number_1_beginning_byte:number_1_ending_byte])[0]
#                        if event_number > max_number:
#                            max_number = event_number
#                        if event_number < min_number:
#                            min_number = event_number
                codes_array = tools.unpacked_from_bytes(
                    '<64h', chunk[codes_beginning_byte:codes_ending_byte])
                for i in range(0, len(cleaned_amplitudes), 3):
                    if codes_array[2*(i//3)] <= 1800:
                        cleaned_amplitudes[i] =\
                        codes_array[2*(i//3)]/4 - peds_array[2*(i//3)]
                        cleaned_amplitudes[i+1] = 0
                    else:
                        cleaned_amplitudes[i] =\
                        codes_array[2*(i//3) + 1]/4 - peds_array[2*(i//3) + 1]
                        cleaned_amplitudes[i+1] = 1
#                    cleaned_amplitudes[i+1] =\
#                    int(bin(codes_array[2*(i//3)])[-1])
#                           ig_status = 0 if BOTH channels IS ignored
#                           1 if LOW IS NOT ignored, HIGH IS ignored
#                           2 if HIGH IS NOT ignored, LOW IS ignored
#                           3 if BOTH IS NOT ignored
                    cleaned_amplitudes[i+2] =\
                    ig_array[2*(i//3)] + ig_array[2*(i//3) + 1]
                cleaned_amplitudes_pack = tools.packed_bytes(
                    'fBB'*32,
                    cleaned_amplitudes)
                        # chunk_size in wfp_file will be 282 bytes
                cleaned_file.write(chunk[:codes_beginning_byte])
                cleaned_file.write(cleaned_amplitudes_pack)
                cleaned_file.write(chunk[codes_ending_byte:])
                        # chunk_size in header will be 17 bytes
#            except Exception:
#                print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
#                    "RAW CHUNK CORRUPTION ERROR!",
#                    chunk_counter,
#                    file_to_process))
                chunk_counter += 1
                chunk = codes_fin.read(chunk_size)
#    return min_number, max_number    



# =============================================================================
#
# =============================================================================
    
