#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:11:45 2020

@author: yaroslav
"""
# =============================================================================
#
# =============================================================================
import manticore_parser
import manticore_tools
import manticore_preprocessing
import manticore_decoding

# ==============================================================================
# All user sets saves in main branch (manticore_main_slow, HERE)===============
# and oly main branch has access to operate them ==============================
# ==============================================================================

print("{0}manticore 3.0 SLOW mode{0}\n\n".format("_"*39))
START_TIME = manticore_tools.what_time_is_now()

SET_0, SET_1, SET_2, SET_3 = manticore_tools.read_input_card()

if SET_0 not in ('', '1'):
    print("ERROR: SET_0 IS WRONG!")
    manticore_tools.system_exit()
if SET_1 not in ('1', '2', '3'):
    print("ERROR: SET_1 IS WRONG!")
    manticore_tools.system_exit()
if SET_2 not in ('', '1'):
    print("ERROR: SET_2 IS WRONG!")
    manticore_tools.system_exit()

if SET_0 == '1':
    print("Preprocess of deleting old temporary files...")
    manticore_tools.mess_destroyer(START_TIME)

manticore_parser.parser(SET_3, START_TIME)
manticore_tools.is_preprocessing_needed(SET_1, START_TIME)
manticore_decoding.to_process_1(START_TIME)
manticore_preprocessing.fill_the_summary_files(START_TIME)

if SET_2 != '1':
    manticore_tools.mess_destroyer(START_TIME)
print("{} {}".format(
    "\nAll time for processing input data:\n",
    manticore_tools.time_check(START_TIME)))
# =============================================================================
#
# =============================================================================
