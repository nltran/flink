import scipy.sparse as sp
import numpy as np
import sys

from sklearn.preprocessing import normalize

DENSITY_VALUES = [1e-3, 1e-2, 1e-1]
NUMBER = 5
NOISE = [0.0, 1e-3, 1e-1]
PB_TYPE = 'random'
M = 10000
N = 1000
weights = [0.2, 0.5, 0.7, 0.3, 0.9]
index = [0, 1, 2, 3, 4]

def main(M, N):

    for n in range(NUMBER):
        for density in DENSITY_VALUES:
            for noise in NOISE:
                x_out = open('data-' + 'noise_' + str(noise) + '-sparsity_' + str(density) + '-expe_' + str(n) + '.csv', 'w')
                y_out = open('target-' + 'noise_' + str(noise) + '-sparsity_' + str(density) + '-expe_' + str(n) + '.csv', 'w')

                if (PB_TYPE == 'random'):
                    X = sp.rand(N, M, density=density)
                    X = normalize(X, axis=0)
                elif (PB_TYPE == 'eye'):
                    X = sp.eye(N, M)
                else:
                    sys.exit('Unknown type of problem.')

                # Vector of weights
                # alpha = sp.coo_matrix((weights, (index, np.zeros(len(weights)))), shape=(M, 1)).tocsr()

                alpha = sp.rand(M, 1, density=1e-2)

                Y = X.dot(alpha) + noise * normalize(np.random.rand(N, 1), axis=0)

                for y in np.array(Y).squeeze():
                    y_out.write(str(y) + '\n')
                y_out.close()

                # Print header
                x_out.write('id, nnz_values\n')

                for i in range(N):
                    row = X[i,:]
                    _, nnz_cols = row.nonzero()
                    x_out.write(str(i) + ',')
                    for col, value in zip(nnz_cols, row.data):
                        x_out.write(str(col) + ':' + str(value) + ' ')
                    x_out.write('\n')

if __name__ == '__main__':
    main(M, N)
