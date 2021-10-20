#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 00:29:35 2021

@author: yaroslav
"""

def fill_the_ummary_file(min_number, max_number, start_time):
    
    print("\nEmpty matrix of events are creating...")
    matrix_of_events = [['']*22 for i in range(max_number + 1 - min_number)]
    print("\nEmpty matrix of events has been created...")
    
    