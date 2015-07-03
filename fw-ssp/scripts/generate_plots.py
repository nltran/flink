import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
from numpy import float64 as f64
from fnmatch import fnmatch

def main(args):
    beta = 1.0
    noise = 0.0
    sparsity = 1e-3
    load = 0
    slacks = [0, 3, 5, 10, 100] if load > 0 else [0, 5]#[0, 3, 5, 10, 25]

    root_dir = args[0]
    column = args[1]

    fig, axes = plt.subplots(1, 2, sharey=True)
    legend = ['staleness='+str(s) for s in slacks]

    filenames = []

    for slack in slacks:
        if load > 0:
            subroot_dir = root_dir+str(beta)+'_'+str(slack)+'_'+str(noise)+'_'+str(sparsity)+'_LOAD_5_'+str(load)
        else:
            subroot_dir = root_dir+str(beta)+'_'+str(slack)+'_'+str(noise)+'_'+str(sparsity)

        filenames = [os.path.join(subroot_dir, sub_dir, worker_file) for sub_dir in os.listdir(subroot_dir) for worker_file in os.listdir(os.path.join(subroot_dir, sub_dir)) if fnmatch(worker_file, '*.csv')]

        dfs = [
            pd.read_csv(
                filename,
                header=None,
                index_col=['iteration', 'workerId'],
                names=[
                    'workerId',
                    'iteration',
                    'atomId',
                    'time',
                    'residual',
                    'dualityGap',
                    'startTime'
                ],
                dtype={'time': f64, 'startTime': f64}
            ) for filename in filenames]

        concatenated = pd.concat(dfs)

        means = concatenated.mean(level=['iteration'])
        # means.time = means.time.cumsum()
        means.startTime = 1e-9 * (means.startTime - means.startTime.iloc[0])
        errors = concatenated.std(level=['iteration'])

        # means.plot(ax=axes[0], y=column, yerr=errors, lw=2.0)
        means.plot(ax=axes[0], y=column, legend=legend)
        # means.plot(ax=axes[1], x='time', y=column)
        means.plot(ax=axes[1], x='startTime', y=column)

    for i in range(len(axes)):
        axes[i].legend(legend)
    axes[0].set_ylabel('Objective')
    axes[1].set_xlabel('Time (s)')

    plt.savefig(column+'_'.join(map(str, [beta, noise, sparsity, 'load='+str(load)])) + '.eps')

if __name__ == '__main__':
    main(sys.argv[1:])
