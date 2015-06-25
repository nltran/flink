import scipy.sparse as sp
import numpy as np
import sys

density = 0.1
M = 10000
N = 1000
weights = [0.2, 0.5, 0.7, 0.3, 0.9]
index = [0, 1, 2, 3, 4]
x_out = open('data.csv', 'w')
y_out = open('target.csv', 'w')
problem_type = 'eye'

def main(M, N):
    if (problem_type == 'random'):
        X = sp.rand(N, M, density=density, random_state=42).tocsc()
        # Making the matrix a {0,1} one
        X.data = np.ones(len(X.data))
    elif (problem_type == 'eye'):
        X = sp.eye(N, M, format='csc')
    else:
        sys.exit('Unknown type of problem.')
    # Vector of weights
    alpha = sp.coo_matrix((weights, (index, np.zeros(len(weights)))), shape=(X.shape[1], 1)).tocsc()

    Y = X.dot(alpha).toarray().squeeze()

    # Y = Y_temp.copy()
    # Y[:] = -1.0
    # Y[Y_temp > 0.5] = 1.0

    for y in Y:
        y_out.write(str(y) + '\n')
    y_out.close()

    # Print header
    x_out.write('id, nnz\n')

    for i in range(X.shape[0]):
        row = X[i,:]
        _, nnz_cols = row.nonzero()
        x_out.write(str(i) + ',')
        for col in nnz_cols:
            x_out.write(str(col) + ' ')
        x_out.write('\n')

if __name__ == '__main__':
    main(M, N)
