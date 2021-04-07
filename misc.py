#########################################################################################
##
##  This module contains helper functions.
##  The funtions in this module must not inherite any of the other local modules.
##
## Author: Lucas Viani
## Date  : 22.10.2018
##

import os, re, math, datetime, copy
import logging
logger = logging.getLogger() 

#########################################################################################
# Date and Time

def last_day_of_month(any_day):
    '''Returns the last day of the month in the date provided.'''
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)


def num_days_month_pd(dt): return num_days_month(dt)


def num_days_month(any_day=None, year=None, month=None):
    '''Return the number of days in the month of the date provided.'''
    if any_day is not None:
        year  = any_day.year
        month = any_day.month
    dt_init = datetime.datetime(year,month,1)
    dt_end  = last_day_of_month(any_day)
    return dt_end.day - dt_init.day + 1


def format_seconds (sec):
    '''Returns a formated string with hours, minutes, and seconds.'''
    # hours
    hours = sec // 3600 
    # remaining seconds
    sec = sec - (hours * 3600)
    # minutes
    minutes = sec // 60
    # remaining seconds
    seconds = sec - (minutes * 60)
    return '{:.0f}h {:.0f}min {:.0f}sec'.format(hours, minutes, seconds)


#########################################################################################
# Other functions
def isnumber (val):
    '''Returns True if input data is a number, and False otherwise.'''
    return re.match(r'^-?\d+(?:\.\d+)?$', str(val))


def get_valid(_dict):
    '''Removes the keys in a dictionary with None as value.'''
    assert isinstance(_dict, dict)
    
    _dict_clean = {}
    if any(v != None for v in iter(_dict.values())):        
        for key, val in _dict.items():
            if val != None:
                _dict_clean[key] = val
    return _dict_clean


def get_key_of_value (dc, val, only_first=None):
    '''Returns the first key matching the values passed.'''
    # Handling the defaut parameters.....................................................
    only_first = only_first if only_first is not None else False
    # Collecting the keys................................................................
    if only_first:
        for k, v in dc.items():
            if v == val:
                return k
        return None
    else:
        lst_key = []
        for k, v in dc.items():
            if v == val:
                lst_key.append(k)
        return lst_key


def sort_dict (d, by_key=True):
    '''Sorts a dictionary.
    
    Parameters
    ----------
    d : dict
        The dictionary to be sorted
    by_key : bool
        Whether to sort by key (True) or value (False).

    Return
    ------
        Returns the sorted dictionary.
    '''
    id = 0 if by_key else 1
    return {t[0]:t[1] for t in sorted(d.items(), key=lambda x: x[id])}


def flatten_values (obj_in):
    ''' Flattens the values of a dictionary, returning a list of the values.
        The values of the dictionary should be lists.

        Parameters
        ----------
        dc : dict
            Dictionary storing lists as values.

        Return
        ------
        Returns a list with the flattened lists stored as values.
    '''
    lst = []
    if isinstance(obj_in, dict):     
        for v in obj_in.values():
            if v is not None:
                lst.extend(v)        
        return lst
    elif isinstance(obj_in, list):
        for v in obj_in:
            if v is not None:
                lst.extend(v)        
        return lst
    return None


def replace_words (fin, fout, dc_replace):
    
    with open(fin,'r+') as textfile, open(fout,'w') as textfile_new:
        for line in textfile:
            new_line = line            
            for k, v in dc_replace.items():                
                if k in line:                    
                    new_line = new_line.replace(k, v)                    
            textfile_new.write(new_line)
    textfile    .close()
    textfile_new.close()


def gp(val):
    '''
    Auxiliary function used while printing messages.
    Returns the 's' character if the integer provided is larger than 1.
    '''
    return 's' if val > 1 else ''


def fix_freq (freq):
    ''' Fix the frequency alias that dont have a number to it.
        It will add the number 1 to the frequency, thus completing alias.
        Some functions of pandas need the unit and number.
    '''
    cmd = re.compile('^(\d+)')
    if(not cmd.findall(freq)):
        return '1'+freq
    return freq


def has_pattern (lst, lst_regex):
    '''
    Selects the items in 'lst' matching at least one pattern in 'lst_patt'.
    The items must be string type.
    
    Parameters
    ----------
    lst : list
        List of string to be searched.
    lst_regex : list
        List of regex commands.
        
    Returns
    -------
        Returns a list with the items matching at least one pattern.
    '''
    # Validating the arguments...........................................................
    assert isinstance(lst_regex, list), 'List expected and '+ type(lst_regex) + ' provided.'
    
    # Compiling the regex commands.......................................................
    lst_regex_comp = []
    for re_cmd in lst_regex:
        lst_regex_comp.append(re.compile(re_cmd))
    
    # Looking for matches................................................................
    lst_match = []
    for v in lst:
        if isinstance(v, str):
            for re_comp in lst_regex_comp:
                res = re_comp.findall(v)
                if res and len(res) > 0:
                    lst_match.append(v)
                    break
        else:
            logger.debug(f'Column label passed as integer. Ignoring it: {v}')
    return lst_match            


def set_blocks(lst, n_blocks):
    """Returns list storing successive n-sized chunks from lst.
    
        Parameters
        ----------
        lst : list
            The list to be splitted.
        n_blocks : int
            The number of blocks to divide the list into.

        Return
        ------
            Returns a list with the blocks.
    """
    assert n_blocks != 0, 'Number of items per block must be larger than ZERO.'
    lst_n     = [ int(len(lst)/n_blocks) ] * n_blocks
    curr_sum  = sum(lst_n)
    n_missing = len(lst) - curr_sum 
    # Distribution the missing entries equally through the first items of the list
    for i in range(0, n_missing):
        lst_n[i] = lst_n[i] + 1

    lst_n = [i for i in lst_n if i > 0]
    # Building the block list
    lst_block = []
    i_init    = 0
    for nitem in lst_n:    
        i_last = i_init + nitem
        lst_block.append(lst[i_init : i_last])
        i_init = i_last

    return lst_block


def group_by_extension (lst_fnm, no_case):
    '''
    Groups the files in the list provided by their extension.
    
    lst_fnm : list
        List of files to be grouped.
    no_case : bool
        Whether to lower the case of the extension before action.
    '''
    fext = {}
    for i, fnm in zip(range(len(lst_fnm)), lst_fnm):    
        ext = os.path.splitext(fnm)[1][1:]
        if no_case: ext = ext.lower()
        if not ext in fext.keys():
            fext[ext] = []
        fext[ext].append(i)
        
    return fext


def rmv_duplicates_dict_of_lists (lst_dict):
    '''
    Remove duplicated entries in the values of a dictionary keeping the last assigned.
    The dictionaries must have only one key, and its value must be a list.
    
    Example
    -------
    [{'t1':[1,2,3]}, {'t2':[2,3,4]}] is transformed to [{'t1':[1]}, {'t2':[2,3,4]}]
    
    TODO
    ----
    Should be generalized to dictionaries with multiple entries.
    
    Paramerters
    -----------
    lst_dict : List of dictionaries. The dictionaries must have only one key, and its value must be a list.
    
    '''
    
    d_final = []
    len_d   = len(lst_dict)
    id      = 0
    for d1 in lst_dict[id:len_d-1]:
        d_tmp = copy.deepcopy(d1)
        key0  = list(d_tmp.keys())[0]

        for d2 in lst_dict[id+1:len_d]:
            d_tmp = {key0 : [c for c in list(d_tmp.values())[0] if not c in list(d2.values())[0]]}
        d_final.append(d_tmp)
        id += 1
    d_final.append(lst_dict[-1])
    return d_final

def is_int (val):
    '''Return True if the string provided is an integer and False otherwise.'''
    return True if re.match("^\d+?", val) is not None else False


def to_numeric (val):
    ''' Converts a single value to numeric if possible.
        It will try to cast the input value to integer and float.    
        Parameters
        ----------
        val : string
            The string to be casted into a numeric type.

        Return
        -------
        Returns the numeric values if the action was successful, or the input string otherwise.
    '''
    if   re.match("^\d+?"       , val) is not None: return int  (val)
    elif re.match("^\d+?\.\d+?$", val) is not None: return float(val)
    return val


def zfill(s, n_pad=3):
    ''' Fills float number with zero padding.    
        Parameters
        ----------
        s : float
            Number to zero pad.
        n_pad : int
            Numver of zero padding

        Return
        -------
        Returns a zero padded string.
    '''
    s_str = str(s) 
    s_len = len(s_str)
    s_dot = s_str.find('.')
    s_dec = s_len - s_dot -1
    s_neg = s_str.find('-')

    is_neg = bool(s_neg >= 0)

    # Check the number of digits before the dot
    s_dig = s_dot if is_neg else s_dot -1    
    # Number of zeros to add
    n_add = n_pad - s_dig

    # Adding zeros
    # sign = 1 if is_neg else 0
    return s_str.zfill(int(is_neg) + n_add + s_dig + 1 + s_dec)


def dms2deg(val, units=True):
    ''' Converts a coordinate (latitud or longitud) from the 
        DMS representation to the Degrees one.

        Parameters
        ----------
        val : str
            The coordinate as string.
        units : bool
            Defines whether the coordinate provided has units. Ex 3°47'06.8"W
            If the coordinate has no units, it is expected to have at least 2 characters per segment. Ex: 034706.8W

        Return
        ------
            Returns the converted coordinate.
    '''
    direction = {'N':1.0, 'S':-1.0, 'E': 1.0, 'W':-1.0}
    if units: coord = val.replace(u'°',' ').replace('\'',' ').replace('"',' ')
    else:     coord = val[0:2] + ' ' + val[2:4] + ' ' + val[4:-1] + ' ' + val[-1:]
    coord    = coord.split()
    cood_dir = coord.pop()
    coord.extend([0,0,0])
    return (float(coord[0]) + float(coord[1])/60.0 + float(coord[2])/3600.0) * direction[cood_dir]
    