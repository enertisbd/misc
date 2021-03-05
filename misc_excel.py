#########################################################################################
##
##  This module contains helper functions.
##  The funtions in this module must not inherite any of the other local modules.
##
## Author: Lucas Viani
## Date  : 22.10.2018
##

import openpyxl

#########################################################################################
#
# Fucntions..............................................................................
#
#
def xls_cell_coord( cell_str ):
    xy  = openpyxl.utils.coordinate_from_string(cell_str)
    col = openpyxl.utils.column_index_from_string(xy[0])
    return [xy[1], col]

def xls_set_range (sheet, cell_str, values):
    coord = xls_cell_coord(cell_str)
    nrow, ncol = values.shape
    for i in range(nrow):
        for j in range(ncol):
            sheet.cell(row=coord[0]+i, column=coord[1]+j).value = values[i,j]

def xls_get_range(rg):
    '''Retrieves the values of the range provided.'''
    #nrow   = len(rg   )
    ncol   = len(rg[0])
    lst_rg = []
    for r in range(0,len(rg)):
        lst_row = []
        for c in range(0, ncol):
            lst_row.append(rg[r][c].value)
        lst_rg.append(lst_row)
    return lst_rg

#########################################################################################
