#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 23:45:35 2020

@author: yaroslav
"""
import manticore_tools as tools

EVENT_FILTER = 0
TOTAL_DICT_OF_DAYS_FILE = ".total_dict_of_days.txt"
# =============================================================================
#
# =============================================================================

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

    tails_set = set()
    for file in files_list:
        if file[:-19] == day:
            file = tools.check_and_cut_the_tail(file)
            tails_set.add(file[60:])
    print("Set of tails have been created.")
    return tails_set
# =============================================================================
#
# =============================================================================

def list_of_tail_files(day_directory, list_of_BSM, tail):

    tail_files = []
    for BSM in list_of_BSM:
        BSM_name = "{}{}/".format(
            day_directory,
            BSM)
        new_tail_file = BSM_name + tools.directory_objects_parser(
            BSM_name,
            tools.TAIL_FILE_REGULAR_PATTERN + tail).split()[0]
        tail_files.append(new_tail_file)
    return tail_files
# =============================================================================
#
# =============================================================================

def clean_the_matrix_of_USER_NUMBER_cluster_events(day_directory, tail, matrix_of_events, min_event_number_in_tail, clean_status = 0):
    """Cleaning the matrix of events for one tail from events where no one
    cluster was """
    empty_event = ['']*22
    event_numbers_parallel_list = []
    new_matrix_of_events = [['']*22 for i in range(len(matrix_of_events))]
    if clean_status == 0:
        out_tail_file = day_directory + tail + '_clean.list'
    elif clean_status == 1:
        out_tail_file = day_directory + tail + '_static.list'
    elif clean_status == 2:
        out_tail_file = day_directory + tail + '_dynamic.list'
        
    with open(out_tail_file, 'w+') as out_tail_file:
#        out_tail_file.write("Tail\tEvent number\tCoins\n")

        for i in range (len(matrix_of_events)):
            not_empty_cell_counter = 0
            for j in range(len(matrix_of_events[i])):
                if matrix_of_events[i][j] != "":
                    not_empty_cell_counter += 1
            if not_empty_cell_counter > EVENT_FILTER:
                new_matrix_of_events[i] = matrix_of_events[i]
                out_tail_file.write("{}\t{}\t{}\n".format(tail, min_event_number_in_tail + i, not_empty_cell_counter))
                event_numbers_parallel_list.append(min_event_number_in_tail + i)
    return [value for value in new_matrix_of_events if value != empty_event], event_numbers_parallel_list
# =============================================================================
#
# =============================================================================

def print_statistics_for_matrix_of_events(matrix_of_events, stat_file):
    """Print the coincidences statistics for every 2 minutes of data
    (for one tail). From 0_BSM events to 22_BSM events.

    Takes the matrix of events in format M[event number][BSM], where
    each item -
    string = maroc number + event time + 64*'amplitude + trigger status + ignore status'"""

    coin = [0]*23
    for string in matrix_of_events:
        string_counter = 0
        for item in string:
            if item != '':
                string_counter += 1
        coin[string_counter] += 1
    with open(stat_file, 'w+') as stat_file:
        for i in range(len(coin)):
            print("coins: {}\tevents: {}\n".format(i, coin[i]))
            stat_file.write("coins: {}\tevents: {}\n".format(i, coin[i]))
# =============================================================================
#
# =============================================================================

def fill_the_matrix_of_events(matrix_of_events, tail_files, tail, tail_max_min_list, start_time, clean_status = 0):

    chunk_size = 282
    tail_files_counter = 0
    for tail_file in tail_files:
        
        if clean_status == 0:
            print("Tail file  {}  clean amplitudes collecting...".format(tail_file))
            tail_file = tools.make_BSM_file_temp(tail_file) + '.amp'
            chunk_size = 281
        elif clean_status == 1:
            print("Tail file  {}  static amplitudes collecting...".format(tail_file))
            tail_file = tools.make_BSM_file_temp(tail_file) + '.asp'
            chunk_size = 282
        elif clean_status == 2:
            print("Tail file  {}  dynamic amplitudes collecting...".format(tail_file))
            tail_file = tools.make_BSM_file_temp(tail_file) + '.adp'
            chunk_size = 282
#        try:
        with open(tail_file, 'rb') as tail_file:
            chunk = tail_file.read(chunk_size)
            chunk_counter = 0
            while chunk:
#                    try:
                head_array = tools.unpacked_from_bytes('hhii', chunk[:12])
                num_event = head_array[2]
                maroc_number = tools.unpacked_from_bytes('h', chunk[20:22])[0]
                time_array = tools.unpacked_from_bytes('hhhh', chunk[12:20])
                ns = (time_array[0] & 0x7f)*10
                mks = (time_array[0] & 0xff80) >> 7
                mks |= (time_array[1] & 1) << 9
#                mls = (time_array[1] & 0x7fe) >> 11 # IF leads to mls = 0 ...
                mls = (time_array[1] & 0x7fe) >> 1 # TRY THIS !!!
                s = (time_array[1] & 0xf800) >> 11
                s |= (time_array[2] & 1) << 5
                m = (time_array[2] & 0x7e) >> 1
                h = (time_array[2] & 0xf80) >> 7
                time_string = "{}:{}:{}.{}.{}.{}".format(h, m, s, mls, mks, ns)
                        
                if clean_status == 0:                        
                    result_array = tools.unpacked_from_bytes('fB'*32, chunk[24:-4])
                elif clean_status in (1, 2):
                    result_array = tools.unpacked_from_bytes('fBB'*32, chunk[24:-4])
                            
                result_string_ampls = '\t'.join([str(x) for x in result_array])
                matrix_of_events[num_event - tail_max_min_list[0]][maroc_number] =\
                    "{}\t{}\t{}".format(
                            maroc_number,
                            time_string,
                            result_string_ampls)
                                    
#                    except Exception:
#                        print("{} Chunk number {} in file {} is seems to be corrupted!".format(
#                            "AMPLITUDE FILE CHUNK RECORDING ERROR!",
#                            chunk_counter,
#                            tail_file))
                chunk_counter += 1
                chunk = tail_file.read(chunk_size)

            tail_files_counter += 1
            tools.syprogressbar(
                tail_files_counter,
                len(tail_files),
                u'\u24BB',
                "tail files {} amplitudes collecting".format(tail),
                start_time)
#        except Exception:
#            print("{} File {} is seems to be not existed!".format(
#                    "AMPLITUDE FILE EXISTING ERROR!",
#                    tail_file))
    return matrix_of_events
# =============================================================================
#
# =============================================================================

def create_summary_file_for_tail(tail, tail_max_min_list, start_time,
                                 list_of_BSM, day_directory,
                                 tails_counter, list_of_tails):
    
    min_event_number_in_tail = tail_max_min_list[0]
    max_event_number_in_tail = tail_max_min_list[1]

    print("\nEmpty matrix of clean events are creating...")
    matrix_of_events_clean = [['']*22 for i in range(max_event_number_in_tail - min_event_number_in_tail + 1)]
    print("\nEmpty matrix of events has been created...")

    print("\nFiles list for tail  {}  from  {}  are creating...".format(tail, day_directory))
    tail_files = list_of_tail_files(day_directory, list_of_BSM, tail)
    


    print("\nEmpty matrix of events with static pedestals cleaning are creating...")
#    matrix_of_events_static = [['']*22 for i in range(max_event_number_in_tail - min_event_number_in_tail + 1)]
    print("\nEmpty matrix of events has been created...")

    print("\nFiles list for tail  {}  from  {}  are creating...".format(tail, day_directory))
    tail_files = list_of_tail_files(day_directory, list_of_BSM, tail)
    


    print("\nEmpty matrix of events with dynamic pedestals cleaning are creating...")
    matrix_of_events_dynamic = [['']*22 for i in range(max_event_number_in_tail - min_event_number_in_tail + 1)]
    print("\nEmpty matrix of events has been created...")
        
    print("\nFiles list for tail  {}  from  {}  are creating...".format(tail, day_directory))
    tail_files = list_of_tail_files(day_directory, list_of_BSM, tail)



    print("Event matrix with static pedestals cleaning for tail  {}  from  {}  are creating...".format(tail, day_directory))
#    matrix_of_events_static = fill_the_matrix_of_events(matrix_of_events_static, tail_files, tail, tail_max_min_list, start_time, 1)
    
    print("Event matrix with dynamic pedestals cleaning for tail  {}  from  {}  are creating...".format(tail, day_directory))
    matrix_of_events_dynamic = fill_the_matrix_of_events(matrix_of_events_dynamic, tail_files, tail, tail_max_min_list, start_time, 2)

    print("Event matrix with clean amplitudes for tail  {}  from  {}  are creating...".format(tail, day_directory))
    matrix_of_events_clean = fill_the_matrix_of_events(matrix_of_events_clean, tail_files, tail, tail_max_min_list, start_time)

    print("\nMatrix for tail {} from {} are cleaning for less then USER_NUMBER-coincidences events...".format(tail, day_directory))
#    before_user_cleaning_static = len(matrix_of_events_static)
    before_user_cleaning_dynamic = len(matrix_of_events_dynamic)
    before_user_cleaning_clean = len(matrix_of_events_clean)

#    no_user_number_coin_matrix_of_events_static, event_numbers_parallel_list_static = clean_the_matrix_of_USER_NUMBER_cluster_events(day_directory, tail, matrix_of_events_static, min_event_number_in_tail, 1)
    no_user_number_coin_matrix_of_events_dynamic, event_numbers_parallel_list_dynamic = clean_the_matrix_of_USER_NUMBER_cluster_events(day_directory, tail, matrix_of_events_dynamic, min_event_number_in_tail, 2)
    no_user_number_coin_matrix_of_events_clean, event_numbers_parallel_list_clean = clean_the_matrix_of_USER_NUMBER_cluster_events(day_directory, tail, matrix_of_events_clean, min_event_number_in_tail)

#    after_user_cleaning_static = len(no_user_number_coin_matrix_of_events_static)
    after_user_cleaning_dynamic = len(no_user_number_coin_matrix_of_events_dynamic)
    after_user_cleaning_clean = len(no_user_number_coin_matrix_of_events_clean)
    
#    print("DELETED  {:.3f}% events".format((before_user_cleaning_static - after_user_cleaning_static)/before_user_cleaning_static*100))
    print("DELETED  {:.3f}% events".format((before_user_cleaning_dynamic - after_user_cleaning_dynamic)/before_user_cleaning_dynamic*100))
    print("DELETED  {:.3f}% events".format((before_user_cleaning_clean - after_user_cleaning_clean)/before_user_cleaning_clean*100))
        
#    print("Out file for Static amplitudes {} tail from  {}  are filling for user_number-coins...".format(tail, day_directory))
#    with open(day_directory + tail + '_static.out', 'w+') as out_tail_file:
#        for i in range(len(no_user_number_coin_matrix_of_events_static)):
#            out_tail_file.write(
#                "Event_number\t{}\tin_tail_files\t{}\tfor_the\t{}\n".format(
#                    event_numbers_parallel_list_static[i],
#                    tail, day_directory))
#            for j in range(len(no_user_number_coin_matrix_of_events_static[i])):
#                out_tail_file.write("{}\n".format(no_user_number_coin_matrix_of_events_static[i][j]))
#            out_tail_file.write('\n')
            
    print("Out file for Dynamic amplitudes {} tail from  {}  are filling for user_number-coins...".format(tail, day_directory))
    with open(day_directory + tail + '_dynamic.out', 'w+') as out_tail_file:
        for i in range(len(no_user_number_coin_matrix_of_events_dynamic)):
            out_tail_file.write(
                "Event_number\t{}\tin_tail_files\t{}\tfor_the\t{}\n".format(
                    event_numbers_parallel_list_dynamic[i],
                    tail, day_directory))
            for j in range(len(no_user_number_coin_matrix_of_events_dynamic[i])):
                out_tail_file.write("{}\n".format(no_user_number_coin_matrix_of_events_dynamic[i][j]))
            out_tail_file.write('\n')
            
    print("Out file for Clean amplitudes {} tail from  {}  are filling for user_number-coins...".format(tail, day_directory))
    with open(day_directory + tail + '_clean.out', 'w+') as out_tail_file:
        for i in range(len(no_user_number_coin_matrix_of_events_clean)):
            out_tail_file.write(
                "Event_number\t{}\tin_tail_files\t{}\tfor_the\t{}\n".format(
                    event_numbers_parallel_list_clean[i],
                    tail, day_directory))
            for j in range(len(no_user_number_coin_matrix_of_events_clean[i])):
                out_tail_file.write("{}\n".format(no_user_number_coin_matrix_of_events_clean[i][j]))
            out_tail_file.write('\n')

    tools.syprogressbar(
        tails_counter,
        len(list_of_tails),
        u'\u24C9',
        "creating summary files for tails",
        start_time)
    
    
#    stat_file_static = day_directory + tail + '_static.stat'
    stat_file_dynamic = day_directory + tail + '_dynamic.stat'
    stat_file_clean = day_directory + tail + '_clean.stat'
    print("Statistics for static amplitudes for tail {} from {} are calculating...".format(tail, day_directory))
#    print_statistics_for_matrix_of_events(matrix_of_events_static, stat_file_static)
    print("Statistics for dynamic amplitudes for tail {} from {} are calculating...".format(tail, day_directory))
    print_statistics_for_matrix_of_events(matrix_of_events_dynamic, stat_file_dynamic)
    print("Statistics for clean amplitudes for tail {} from {} are calculating...".format(tail, day_directory))
    print_statistics_for_matrix_of_events(matrix_of_events_clean, stat_file_clean)
# =============================================================================
#
# =============================================================================

def fill_the_summary_files(start_time):   
    """Fill the final summary files with events named (tail).sum.

    Here each existed tail.sum file is being filled by cleaned amplitudes.
    For each file function runs through all (22) repacking and cleaned data
    files with this tail in this day directory. For each data file function
    one by one reads numbers of events. Each block of data function puts to
    the correspondent place in (tail).sum file. This place is the N_1-th string
    in N_2-th blank, where N_1 - BSM number, N_2 - event number. So, this
    string in the tail.sum file will contain exatly amplitudes of THIS BSM in
    THIS event.
    Finally each tail.sum file contains full information about every event
    from two minutes that corresponds to this tail: number, and amplitudes
    of every BSM, also the time of event in every BSM and trigger-status
    and ignore-status of every channel in every BSM."""

    dict_of_days = {}
    with open (TOTAL_DICT_OF_DAYS_FILE, "r") as total_dict_of_days_file:
        day = total_dict_of_days_file.readline()
        while day:
            day = tools.check_and_cut_the_tail(day)
            dict_of_days[day] = {}
            number_of_tails = int(total_dict_of_days_file.readline())
            for i in range (number_of_tails):
                current_tail_record = total_dict_of_days_file.readline().split()
                dict_of_days[day][current_tail_record[0]] = [int(current_tail_record[1]), int(current_tail_record[2])]
            day = total_dict_of_days_file.readline()

    print("The summary files of events are fillng by data...")
    list_of_tails = []
    for day_directory, tail_dict in dict_of_days.items():
        tail_max_min_list = []
        for tail, max_min_list in tail_dict.items():
            list_of_tails.append(tail)
            tail_max_min_list.append(max_min_list)
        list_of_BSM = tools.directory_objects_parser(
            day_directory, tools.BSM_REGULAR_PATTERN).split()

        tails_counter = 0
        for i in range(len(list_of_tails)):
            print("The {} is analizyng...".format(list_of_tails[i]))
            tails_counter += 1
            create_summary_file_for_tail(list_of_tails[i], tail_max_min_list[i], start_time,
                                         list_of_BSM, day_directory,
                                         tails_counter, list_of_tails)
        print("The summary files for  {}  have been created".format(day_directory))
        print(tools.time_check(start_time))


        print("Merging .list files into one...")
        merge_list_files(day_directory)
        print("Global .list file has been created.")

    print(tools.time_check(start_time))
# =============================================================================
#
# =============================================================================

def merge_list_files(day_directory):
    list_files = tools.directory_objects_parser(day_directory, tools.LIST_FILE_PATTERN).split()
    static_file = day_directory + "/static_events_list.txt"
    dynamic_file = day_directory + "/dynamic_events_list.txt"
    clean_file = day_directory + "/clean_events_list.txt"
    
    with open (static_file, "w+") as static_out_file:
        with open(dynamic_file, "w+") as dynamic_out_file:
            with open(clean_file, "w+") as clean_out_file:
                static_out_file.write("Tail\tEvent_number\tCoins\n")
                dynamic_out_file.write("Tail\tEvent_number\tCoins\n")
                clean_out_file.write("Tail\tEvent_number\tCoins\n")

                for file in list_files:
                    if "static" in file:
                        with open (day_directory + "/" + file, "r") as in_file:
                            event_strings = in_file.readlines()
                            for line in event_strings:
                                static_out_file.write(line)
                    if "dynamic" in file:
                        with open (day_directory + "/" + file, "r") as in_file:
                            event_strings = in_file.readlines()
                            for line in event_strings:
                                dynamic_out_file.write(line)
                    if "clean" in file:
                        with open (day_directory + "/" + file, "r") as in_file:
                            event_strings = in_file.readlines()
                            for line in event_strings:
                                clean_out_file.write(line)
# =============================================================================
#
# =============================================================================
