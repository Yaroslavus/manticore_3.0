#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 23 21:14:00 2020

@author: yaroslav
"""

import re
import manticore_tools as tools

DAY_PARSING_PATTERN = re.compile(r"(\/\d{6}.\d{2}|\/\d{6})[^\/\.\d]")
BSM_PARSING_PATTERN = re.compile(r"(\/\d{6}.?\d?\d?\/BSM\d{2})[^\/]")
RAW_FILE_PARSING_PATTERN = re.compile(r"(\/\d{6}.\d{2}\/BSM\d{2}\/\d{8}.\d{3}|\/\d{6}\/BSM\d{2}\/\d{8}.\d{3})")
TAIL_PARSING_PATTERN = re.compile(r"(\/\d{6}\/.\d{3}|\/\d{6}.\d{2}\/.\d{3})")
# =============================================================================
#
# =============================================================================

def parser(string_with_objects_to_process, start_time):

    print("The list of files to process are compiling...")
    all_data = True if string_with_objects_to_process == 'a' else False
    list_of_bsm = re.findall(BSM_PARSING_PATTERN, string_with_objects_to_process + ' ')
    list_of_files = re.findall(RAW_FILE_PARSING_PATTERN, string_with_objects_to_process + ' ')
    list_of_tails = re.findall(TAIL_PARSING_PATTERN, string_with_objects_to_process + ' ')
    list_of_days = re.findall(DAY_PARSING_PATTERN, string_with_objects_to_process + ' ')
    if (not list_of_bsm) and (not list_of_files) and (not list_of_tails) and (not list_of_days) and (not all_data):
        print("ERROR: SET_3 IS FILLED WRONG!")
        tools.system_exit()

    files_list = open('.files_list.txt', 'w+', encoding="utf-8")
    files_list.close()
    with open(".mess.txt", "w+", encoding="utf-8") as mess_file:
        mess_file.write(
            "Made temporary file:  {}/.files_list.txt\n".format(
                tools.SCRIPT_DIRECTORY))

    for file in list_of_files:
        parse_one_file(file)
    for bsm in list_of_bsm:
        parse_one_BSM(bsm)
    for tail in list_of_tails:
        parse_one_tail(tail)
    for day in list_of_days:
        parse_one_day(day)
    if all_data:
        parse_all_data()

    print("{} {}".format(
        "The list of files to process was made.",
        "It's in the script directory under the name  .files_list.txt"))

    print("Parsing finished.")
    print(tools.time_check(start_time))
# =============================================================================
#
# =============================================================================

def parse_one_file(relative_file_path):

    with open(".files_list.txt", "a") as files_list:
        abs_file_path = "{}{}".format(
            tools.data_dir(),
            relative_file_path)
        if tools.is_exist(abs_file_path):
            files_list.write("{}\n".format(abs_file_path))
        else: print("{} DOES NOT EXIST!".format(abs_file_path))
# =============================================================================
#
# =============================================================================

def parse_one_BSM(BSM_relative_path):

    with open(".files_list.txt", "a") as files_list:
        BSM_abs_path = "{}{}/".format(
            tools.data_dir(),
            BSM_relative_path)
        if tools.is_exist(BSM_abs_path):
            list_of_files = tools.directory_objects_parser(
                BSM_abs_path,
                tools.RAW_FILE_REGULAR_PATTERN).split()
            for file_relative_path in list_of_files:
                files_list.write(
                    "{}{}\n".format(
                        BSM_abs_path,
                        file_relative_path))
        else: print("{} DOES NOT EXIST!".format(BSM_abs_path))
# =============================================================================
#
# =============================================================================

def parse_one_tail(tail_relative_path):

    with open(".files_list.txt", "a") as files_list:
        day_and_tail_name = tail_relative_path.split('.')
        day_abs_path = "{}{}".format(
            tools.data_dir(),
            day_and_tail_name[0])
        list_of_BSM = tools.directory_objects_parser(
            day_abs_path,
            tools.BSM_REGULAR_PATTERN).split()
        for BSM_relative_path in list_of_BSM:
            BSM_abs_path = "{}{}/".format(
                day_abs_path,
                BSM_relative_path)
            list_of_files = tools.directory_objects_parser(
                BSM_abs_path, "{}{}$".format(
                    tools.TAIL_FILE_REGULAR_PATTERN,
                    day_and_tail_name[1])).split()
            for file_relative_path in list_of_files:
                files_list.write(
                    "{}{}\n".format(
                        BSM_abs_path,
                        file_relative_path))
# =============================================================================
#
# =============================================================================

def parse_one_day(day_relative_path):

    with open(".files_list.txt", "a") as files_list:
        day_abs_path = "{}{}/".format(
            tools.data_dir(),
            day_relative_path)
        list_of_BSM = tools.directory_objects_parser(
            day_abs_path,
            tools.BSM_REGULAR_PATTERN).split()
        for BSM_relative_path in list_of_BSM:
            BSM_abs_path = "{}{}/".format(
                day_abs_path,
                BSM_relative_path)
            list_of_files = tools.directory_objects_parser(
                BSM_abs_path,
                tools.RAW_FILE_REGULAR_PATTERN).split()
            for file_relative_path in list_of_files:
                files_list.write("{}{}\n".format(
                    BSM_abs_path,
                    file_relative_path))
# =============================================================================
#
# =============================================================================

def parse_all_data():

    with open(".files_list.txt", "a") as files_list:

        list_of_days = tools.directory_objects_parser(
            "{}".format(tools.data_dir()),
            tools.DAY_REGULAR_PATTERN).split()
        for day_relative_path in list_of_days:
            day_abs_path = "{}/{}/".format(
                tools.data_dir(),
                day_relative_path)
            list_of_BSM = tools.directory_objects_parser(
                day_abs_path,
                tools.BSM_REGULAR_PATTERN).split()
            for BSM_relative_path in list_of_BSM:
                BSM_abs_path = "{}{}/".format(
                    day_abs_path,
                    BSM_relative_path)
                list_of_files = tools.directory_objects_parser(
                    BSM_abs_path,
                    tools.RAW_FILE_REGULAR_PATTERN).split()
                for file_relative_path in list_of_files:
                    files_list.write("{}{}\n".format(
                        BSM_abs_path,
                        file_relative_path))
# =============================================================================
#
# =============================================================================