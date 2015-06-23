#!/usr/bin/env python
# coding: utf-8

def main():
    from optparse import OptionParser

    options_parser = OptionParser()
    options_parser.add_option('-f', '--file1', type='string', action='store', dest='f1', help='First result file.')
    options_parser.add_option('-g', '--file2', type='string', action='store', dest='f2', help='Second result file.')

    (options, args) = options_parser.parse_args()

    if options.f1 is None or options.f2 is None:
        options_parser.print_help()
        exit(1)

    from numpy import zeros
    from numpy.linalg import norm
    from pandas import read_csv
    from scipy.sparse import coo_matrix
    df1 = read_csv(options.f1, header=None, index_col=0, names=['values'])
    df2 = read_csv(options.f2, header=None, index_col=0, names=['values'])

    m1 = coo_matrix((df1['values'].values, (df1.index, zeros(df1.size))), shape=(100000, 1)).tocsr()
    m2 = coo_matrix((df2['values'].values, (df2.index, zeros(df2.size))), shape=(100000, 1)).tocsr()

    print(abs(m1.transpose().dot(m2))/(norm(m1.toarray()) * norm(m2.toarray())))

if __name__ == '__main__':
    main()
