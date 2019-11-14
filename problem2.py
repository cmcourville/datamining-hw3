from mrjob.job import MRJob
import numpy as np
import logging
#-------------------------------------------------------------------------
'''
    Problem 2: 
    In this problem, you will use mapreduce framework to implement matrix multiplication. 
    
    Matrix Dataset:
    Suppose we have a dataset of two matrices A and B (matrix.csv), each line represents an element in matrix A or matrix B.
    For example, to represent a (2X3) matrix A = 1,2,3
                                                 4,5,6
        and a (3X2) matrix B = 1,-1
                               1,-1
                               1,-1
        We will have the following input file:

        Matrix, Row_index, Column_index, Value, num_rows_C, num_columns_C
        ------------------------------------------------------------------
          A   ,    1     ,      1      ,   1  ,     2     ,     2
          A   ,    1     ,      2      ,   2  ,     2     ,     2
          A   ,    1     ,      3      ,   3  ,     2     ,     2
          A   ,    2     ,      1      ,   4  ,     2     ,     2
          A   ,    2     ,      2      ,   5  ,     2     ,     2
          A   ,    2     ,      3      ,   6  ,     2     ,     2
          B   ,    1     ,      1      ,   1  ,     2     ,     2
          B   ,    1     ,      2      ,  -1  ,     2     ,     2
          B   ,    2     ,      1      ,   1  ,     2     ,     2
          B   ,    2     ,      2      ,  -1  ,     2     ,     2
          B   ,    2     ,      1      ,   1  ,     2     ,     2
          B   ,    2     ,      2      ,  -1  ,     2     ,     2
    Here num_rows_C (num_column_C) represents the number of rows (columns) in matrix C.
    We want to compute the matrix C = A X B (matrix multiplication)
    C =  6, -6
        15, -15 

        The outputs of the reducers should be:
       KEY   , VALUE
    ------------------
    ("C",1,1),   6         (which means that The element in the 1st row and 1st column of matrix C is 6)
    ("C",1,2),  -6         (which means that The element in the 1st row and 2nd column of matrix C is -6)
    ("C",2,1),  15         
    ("C",2,2), -15



        -----------------------------------------------------------------------------------------
        This problem is not easy to solve, so let's solve it step by step.
        We will start with a simple case when matrix A (1x1) multiplies matrix B (1x1)
                    A = 3
                    B = 5
                    C = A X B = 15

'''

#--------------------------
class MatMul_1by1(MRJob):
#--------------------------
    ''' 
        Given a matrix A (1 x 1) and a matrix B (1 x 1), compute the product A*B = C (matrix multiplication)
        For example, if the input matrices are 
                    A = 3
                    B = 5
        Then the input value of the mapper will be either 
                                    (1) a text string "A,1,1,3,1,1," represents that 
                                           this is an element of matrix A, 
                                                      in the first row (1), 
                                                      the first column (1),
                                           the value of the element is (3),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C
                                or (2) a text string "B,1,1,5,1,1," represents that 
                                           this is an element of matrix B, 
                                                      in the first row (1), 
                                                      the first column (1),
                                           the value of the element is (5),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C        
        The result:
                    C = A X B = 3*5 = 15
        The output of the reducer should be a key-value pair:   ("C",1,1), 15
        Here ("C",1,1) is the output key, indicating that this result is the element in the first row and first column of matrix C.
        The output value is 15.
    '''

    #----------------------
    def mapper(self, in_key, in_value):
        ''' 
            mapper function, which process a key-value pair in the data and generate intermediate key-value pair(s)
            Input:
                    in_key: the key of a data record (in this example, can be ignored)
                    in_value: the value of a data record, it is a line of text string.
                              The string represents either an element of matrix A or an element of matrix B.

            Yield: 
                    (out_key, out_value) :intermediate key-value pair(s). You need to design the format and meaning of the key-value pairs. These intermediate key-value pairs will be feed to reducers, after grouping all the values with a same key into a value list.
            Hint: To debug in the mapper or reducer, instead of using print() function, you could use logging.warning("debug message") to print information.
        '''
        
        #########################################
        ## INSERT YOUR CODE HERE
        # process input value 

        m, r_0, c_0, eltVal, r_N, col_N = in_value.split(',')
        yield in_key, float(eltVal)

        #########################################


    #----------------------
    def reducer(self, in_key, in_values):
        ''' 
            reducer function, which processes a key and value list and produces output key-value pair(s)
            Input:
                    in_key: an intermediate key from the mapper
                    in_values: a generator of values , which contains all the intermediate values with the same key (in_key) generated by all mappers
            Yield: 
                    (out_key, out_value) : output key-value pair(s). 
            Hint: in_values is a generator object, similar to list, but cannot be directly accessed with indices, such as in_values[0]. 
                  You can use for-loop to iterate through the values in the generator.
        '''
        #########################################
        ## INSERT YOUR CODE HERE

        out_value = 1
        for i in in_values:
            out_value*= i
        yield ('C',1,1), out_value

        #########################################

        #-------------------
        ''' 
            TEST: Now you can test the correctness of your code above by typing `nosetests -v test2.py:test_1_1' in the terminal.

        '''






''' 
 -----------------------------------------------------------------------------------------
    Now let's consider a harder problem: when matrix A (1x2) multiplies matrix B (2x1)
            A = 1,2 

            B = 3
                4

            C = A X B = (1*3 + 2*4) = 11
'''
#--------------------------
class MatMul_1by2(MRJob):
#--------------------------
    ''' 
        Given a matrix A (1 x 2) and a matrix B (2 x 1), compute the product A*B = C (matrix multiplication)
        For example, if the input matrices are 
            A = 1,2 

            B = 3
                4
        Then the input value of the mapper will be either 
                                    (1) a text string "A,1,1,1,1,1," represents that 
                                           this is an element of matrix A, 
                                                      in the first row (1), 
                                                      the first column (1),
                                           the value of the element is (1),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C
                                or (2) a text string "A,1,2,2,1,1," represents that 
                                           this is an element of matrix A, 
                                                      in the first row (1), 
                                                     the second column (2),
                                           the value of the element is (2),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C
                                or (3) a text string "B,1,1,3,1,1," represents that 
                                           this is an element of matrix B, 
                                                      in the first row (1), 
                                                      the first column (1),
                                           the value of the element is (3),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C        
                                or (4) a text string "B,2,1,4,1,1," represents that 
                                           this is an element of matrix B, 
                                                     in the second row (1), 
                                                      the first column (1),
                                           the value of the element is (4),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C  
        The result:
                    C = A X B = (1*3 + 2*4) = 11
        The output of the reducer should be a key-value pair:   ("C",1,1), 11
        Here ("C",1,1) is the output key, indicating that this result is the element in the first row and first column of matrix C.
        The output value is 11.
    '''
    #----------------------
    def mapper(self, in_key, in_value):
        ''' 
            mapper function, which process a key-value pair in the data and generate intermediate key-value pair(s)
            Input:
                    in_key: the key of a data record (in this example, can be ignored)
                    in_value: the value of a data record, it is a line of text string.
                              The string represents either an element of matrix A or an element of matrix B.
                              For example, a text string "A,1,2,2,1,1," represents that 
                                           this is an element of matrix A, 
                                                      in the first row (1), 
                                                     the second column (2),
                                           the value of the element is (2),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C
                              A text string "B,2,1,4,1,1," represents that 
                                           this is an element of matrix B, 
                                                     in the second row (2), 
                                                      the first column (1),
                                           the value of the element is (4),
                                                              there is (1) row in the matrix C
                                                              there is (1) column in the matrix C
            Yield: 
                    (out_key, out_value) :intermediate key-value pair(s). You need to design the format and meaning of the key-value pairs. These intermediate key-value pairs will be feed to reducers, after grouping all the values with a same key into a value list.
            Hint: To debug in the mapper or reducer, instead of using print() function, you could use logging.warning("debug message") to print information.
        '''
        
        #########################################
        ## INSERT YOUR CODE HERE
        # process input value 

        m, r_0, c_0, eltVal, r_N, col_N = in_value.split(',')
        a, b, c = int(r_0), int(c_0), float(eltVal)
        yield in_key, (m, a, b, c) 

        #########################################


    #----------------------
    def reducer(self, in_key, in_values):
        ''' 
            reducer function, which processes a key and value list and produces output key-value pair(s)
            Input:
                    in_key: an intermediate key from the mapper
                    in_values: a generator of values , which contains all the intermediate values with the same key (in_key) generated by all mappers
            Yield: 
                    (out_key, out_value) : output key-value pair(s). 
            Hint: in_values is a generator object, similar to list, but cannot be directly accessed with indices, such as in_values[0]. 
                  You can use for-loop to iterate through the values in the generator.
        '''
        #########################################
        ## INSERT YOUR CODE HERE

        matrixA = np.zeros(5)
        matrixB = np.zeros(5)

        for i, a, b, c in in_values:
            if i=='A':
                matrixA[b-1] = c
            else:
                matrixB[a-1] = c

        yield ('C',1,1), np.dot(matrixA, matrixB)

        #########################################

        #-------------------
        ''' 
            TEST: Now you can test the correctness of your code above by typing `nosetests -v test2.py:test_1_2' in the terminal.

        '''




''' 
 -----------------------------------------------------------------------------------------
    Now let's consider a similar problem: when matrix A (2x1) multiplies matrix B (1x2)
                    A = 1
                        2

                    B = 3, 4
            Then the matrix C should be a 2x2 matrix:
            C = A X B = 3, 4
                        6, 8   
'''
#--------------------------
class MatMul_2by1(MRJob):
#--------------------------
    ''' 
        Given a matrix A (2 x 1) and a matrix B (1 x 2), compute the product A*B = C (matrix multiplication)
        For example, if the input matrices are 
                    A = 1
                        2

                    B = 3, 4
        Then the input value of the mapper will be either 
                                    (1) a text string "A,1,1,1,2,2," represents that 
                                           this is an element of matrix A, 
                                                      in the first row (1), 
                                                      the first column (1),
                                           the value of the element is (1),
                                                              there is (2) rows in the matrix C
                                                              there is (2) columns in the matrix C
                                or (2) a text string "A,2,1,2,2,2," represents that 
                                           this is an element of matrix A, 
                                                     in the second row (2), 
                                                      the first column (1),
                                           the value of the element is (2),
                                                              there is (2) rows in the matrix C
                                                              there is (2) columns in the matrix C
                                or (3) a text string "B,1,1,3,2,2," represents that 
                                           this is an element of matrix B, 
                                                      in the first row (1), 
                                                      the first column (1),
                                           the value of the element is (3),
                                                              there is (2) rows in the matrix C
                                                              there is (2) columns in the matrix C
                                or (4) a text string "B,1,2,4,2,2," represents that 
                                           this is an element of matrix B, 
                                                      in the first row (1), 
                                                     the second column (2),
                                           the value of the element is (4),
                                                              there is (2) rows in the matrix C
                                                              there is (2) columns in the matrix C
        Then the result (matrix C) should be a 2x2 matrix:
        C = A X B = 3, 4
                    6, 8  
        The output of the reducer should have four key-value pairs:   
                   Key     Value
            -------------|--------
                ("C",1,1)| 3
                ("C",1,2)| 4
                ("C",2,1)| 6
                ("C",2,2)| 8
            -------------|--------
    '''
    #----------------------
    def mapper(self, in_key, in_value):
        ''' 
            mapper function, which process a key-value pair in the data and generate intermediate key-value pair(s)
            Input:
                    in_key: the key of a data record (in this example, can be ignored)
                    in_value: the value of a data record, it is a line of text string.
                              The string represents either an element of matrix A or an element of matrix B.
            Yield: 
                    (out_key, out_value) :intermediate key-value pair(s). You need to design the format and meaning of the key-value pairs. These intermediate key-value pairs will be feed to reducers, after grouping all the values with a same key into a value list.
            Hint: To debug in the mapper or reducer, instead of using print() function, you could use logging.warning("debug message") to print information.
        '''
        
        #########################################
        ## INSERT YOUR CODE HERE
        # process input value 

        m, r_0, c_0, eltVal, r_N, col_N = in_value.split(',')
        a, b, c = int(r_0), int(c_0), float(eltVal)
        if m == "A":
            ###logging.warning("in if")
            yield ('C', a, 1), ("A", a, b, c) 
            yield ('C', a, 2), ("A", a, b, c) 
            
        else:
            ###logging.warning("in else")
            yield ('C', 1, b), ("B", a, b, c) 
            yield ('C', 2, b), ("B", a, b, c) 
            

        #########################################


    #----------------------
    def reducer(self, in_key, in_values):
        ''' 
            reducer function, which processes a key and value list and produces output key-value pair(s)
            Input:
                    in_key: an intermediate key from the mapper
                    in_values: a generator of values , which contains all the intermediate values with the same key (in_key) generated by all mappers
            Yield: 
                    (out_key, out_value) : output key-value pair(s). 
            Hint: in_values is a generator object, similar to list, but cannot be directly accessed with indices, such as in_values[0]. 
                  You can use for-loop to iterate through the values in the generator.
        '''
        #########################################
        ## INSERT YOUR CODE HERE

        for i, a, b, c in in_values:
            if i == 'A':
                m_A = c
            else: 
                m_B = c
        out_value = m_A*m_B
        yield in_key, out_value

        #########################################

        #-------------------
        ''' 
            TEST: Now you can test the correctness of your code above by typing `nosetests -v test2.py:test_2_1' in the terminal.

        '''



''' 
 -----------------------------------------------------------------------------------------
    Now let's consider a harder case: when matrix A (2x3) multiplies matrix B (3x2)
                    A = 1, 2, 3
                        4, 5, 6

                    B = 1,-1
                        1,-1
                        1,-1

            Then the matrix C should be a 2x2 matrix:
            C = A X B = 6, -6
                        15, -15  
'''
#--------------------------
class MatMul_2by2(MRJob):
#--------------------------
    ''' 
        Given a matrix A (2 x 3) and a matrix B (3 x 2), compute the product A*B = C (matrix multiplication)
                    A = 1, 2, 3
                        4, 5, 6

                    B = 1,-1
                        1,-1
                        1,-1
        The result (matrix C) should be a 2x2 matrix:
            C = A X B = 6, -6
                        15, -15  
        The output of the reducer should have four key-value pairs:   
                   Key     Value
            -------------|--------
                ("C",1,1)| 6       = 1*1 + 2*1 + 3*1 
                ("C",1,2)|-6       = 1*(-1) + 2*(-1) + 3*(-1)
                ("C",2,1)| 15      = 4*1 + 5*1 + 6*1
                ("C",2,2)|-15      = 4*(-1) + 5*(-1) + 6*(-1)
            -------------|--------
    '''
    #----------------------
    def mapper(self, in_key, in_value):
        ''' 
            mapper function, which process a key-value pair in the data and generate intermediate key-value pair(s)
            Input:
                    in_key: the key of a data record (in this example, can be ignored)
                    in_value: the value of a data record, it is a line of text string.
                              The string represents either an element of matrix A or an element of matrix B.
            Yield: 
                    (out_key, out_value) :intermediate key-value pair(s). You need to design the format and meaning of the key-value pairs. These intermediate key-value pairs will be feed to reducers, after grouping all the values with a same key into a value list.
            Hint: To debug in the mapper or reducer, instead of using print() function, you could use logging.warning("debug message") to print information.
        '''
        
        #########################################
        ## INSERT YOUR CODE HERE

        m, r_0, c_0, eltVal, r_N, col_N = in_value.split(',')
        a, b, c = int(r_0), int(c_0), float(eltVal)
        if m == "A":
            ###logging.warning("in if")
            yield ('C', a, 1), ("A", a, b, c) 
            yield ('C', a, 2), ("A", a, b, c) 
        else:
            #logging.warning("in else")
            yield ('C', 1, b), ("B", a, b, c) 
            yield ('C', 2, b), ("B", a, b, c) 

        #########################################


    #----------------------
    def reducer(self, in_key, in_values):
        ''' 
            reducer function, which processes a key and value list and produces output key-value pair(s)
            Input:
                    in_key: an intermediate key from the mapper
                    in_values: a generator of values , which contains all the intermediate values with the same key (in_key) generated by all mappers
            Yield: 
                    (out_key, out_value) : output key-value pair(s). 
            Hint: in_values is a generator object, similar to list, but cannot be directly accessed with indices, such as in_values[0]. 
                  You can use for-loop to iterate through the values in the generator.
        '''
        #########################################
        ## INSERT YOUR CODE HERE

        m_A =np.zeros(5)
        m_B =np.zeros(5)

        for i, a, b, c in in_values:
            if i == 'A':
                m_A[b-1] = c
            else:
                m_B[a-1] = c
        out_value = np.dot(m_A,m_B)
        yield in_key, out_value

        #########################################

        #-------------------
        ''' 
            TEST: Now you can test the correctness of your code above by typing `nosetests -v test2.py:test_2_2' in the terminal.

        '''

''' 
 -----------------------------------------------------------------------------------------
    Now let's consider the general case: when matrix A (m x k) multiplies matrix B (k x n)
            Then the matrix C should be a (m x n) matrix:
            C = A X B 
'''


#--------------------------
class MatMul(MRJob):
#--------------------------
    ''' 
        Given a matrix A and a matrix B, compute the product A*B = C (matrix multiplication)
    '''

    #----------------------
    def mapper(self, in_key, in_value):
        ''' 
            mapper function, which process a key-value pair in the data and generate intermediate key-value pair(s)
            Input:
                    in_key: the key of a data record (in this example, can be ignored)
                    in_value: the value of a data record, (in this example, it is a line of text string in the data file, check 'matrix.csv' for example)
            Yield: 
                    (out_key, out_value) :intermediate key-value pair(s). You need to design the format and meaning of the key-value pairs. These intermediate key-value pairs will be feed to reducers, after grouping all the values with a same key into a value list.
            Hint: To debug in the mapper or reducer, instead of using print() function, you could use logging.warning("debug message") to print information.
        '''
        
        #########################################
        ## INSERT YOUR CODE HERE
        # process input value 

        m, r_0, c_0, eltVal, r_N, col_N = in_value.split(',')
        a, b, c, d, e = int(r_0), int(c_0), float(eltVal), int(r_N), int(col_N)

        # generate output key-value pairs 
        if m == 'A':
            for k in range(1, e+1):
                #loggin.warning("in A forloop")
                yield ('C', a, k), ('A', a, b, c) 
        else:
            for k in range(1, d+1):
                #loggin.warning("in else forloop")
                yield ('C', k, b), ('B', a, b, c) 

        #########################################

    #----------------------
    def reducer(self, in_key, in_values):
        ''' 
            reducer function, which processes a key and value list and produces output key-value pair(s)
            Input:
                    in_key: an intermediate key from the mapper
                    in_values: a list (generator) of values , which contains all the intermediate values with the same key (in_key) generated by all mappers
            Yield: 
                    (out_key, out_value) : output key-value pair(s). 
        '''
        #########################################
        ## INSERT YOUR CODE HERE
        
        in_values = list(in_values)
        h = int(len(list(in_values))/ 2)
        m_A = h*[0]
        m_B = h*[0]
        for i in in_values:
            j, a, b, c = i
            if j == 'A':
                #loggin.warning("if condition")
                m_A[b-1] = c
            else:
                #loggin.warning("else condition")
                m_B[a-1] = c
        out_value = 0
        for x, y in zip(m_A, m_B):
            out_value += x*y
        yield in_key, out_value 

        #########################################


        #-------------------
        ''' 
            TEST: Now you can test the correctness of your code above by typing `nosetests -v test2.py:test_rand' in the terminal.
                    We will test a case when matrix A and matrix B are randomly generated and saved to "random_matrix.csv". 
        '''


#--------------------------------------------

''' TEST Problem 2: 
        Now you can test the correctness of all the above functions by typing `nosetests -v test2.py' in the terminal.  

        If your code passed all the tests, you will see the following message in the terminal:
            ----------- Problem 2 (15 points in total)-------------- ... ok
            (3 points) MatMul1x1 ... ok
            (3 points) MatMul1x2 ... ok
            (3 points) MatMul2x1 ... ok
            (3 points) MatMul2x2 ... ok
            (3 points) MatMul random ... ok
            ----------------------------------------------------------------------
            Ran 5 tests in 0.103s            
            OK

'''
#--------------------------------------------




