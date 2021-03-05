#########################################################################################
##
##  This module contains helper functions related mathematics, linear algebra, etc.
##  The funtions in this module must not inherite any of the other local modules.
##
## Author: Lucas Viani
## Date  : 22.10.2018
##

import numpy as np

def smooth(x, window_len=11, window='hanning'):
    """smooth the data using a window with requested size.
    
    This method is based on the convolution of a scaled window with the signal.
    The signal is prepared by introducing reflected copies of the signal 
    (with the window size) in both ends so that transient parts are minimized
    in the begining and end part of the output signal.
    
    Parameters
    ----------
        x : numpay array 
            The input signal.
        window_len : int
            The dimension of the smoothing window. It should be an odd integer.
        window : string
            The type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'.
            flat window will produce a moving average smoothing.

    Return
    ------
        Returns a numpy array with the smoothed signal
        
    Example
    -------
    t = linspace(-2,2,0.1)
    x = sin(t)+randn(len(t))*0.1
    y = smooth(x)
    
    See
    ---
    np.hanning, np.hamming, np.bartlett, np.blackman, np.convolve
    scipy.signal.lfilter         
    """ 
     
    if x.ndim != 1:         raise ValueError("smooth only accepts 1 dimension arrays.")
    if x.size < window_len: raise ValueError("Input vector needs to be bigger than window size.")
    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError("Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")  
    if window_len < 3: return x

    s = np.r_[ x[window_len-1:0:-1], x, x[-1:-window_len:-1] ]    
    if window == 'flat': # moving average
          w = np.ones(window_len,'d')
    else: w = eval('np.'+window+'(window_len)')
        
    return np.convolve(w/w.sum(), s, mode='same')

def line_y (x, c, b): return x*c + b
def line_x (y, c, b): return (y-b)/c
    
def line_intersect(m1, b1, m2, b2):
    '''
    Returns the intersection point between two lines.

    Parameters
    ----------
    m1 : float
        The slope of the first line.
    b1 : float
        The intersection of the first line
    m2 : float
        The slope of the second line.
    b2 : float
        The intersection of the second line
    '''
    # Checking for parallel lines
    if m1 == m2: return None
    # y = mx + b
    # Set both lines equal to find the intersection point in the x direction
    # m1 * x + b1 = m2 * x + b2
    # m1 * x - m2 * x = b2 - b1
    # x * (m1 - m2) = b2 - b1
    # x = (b2 - b1) / (m1 - m2)
    x = (b2 - b1) / (m1 - m2)
    # Now solve for y -- use eiÇther line, because they are equal here
    # y = mx + b
    y = m1 * x + b1
    return [x,y]
