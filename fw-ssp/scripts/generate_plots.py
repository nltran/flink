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

    root_dir = args[0]
    column = args[1]
    out_name = args[2]

    exps = [
        'BSP-'+out_name,
        'SSP0-'+out_name,
        'SSP5-'+out_name,
        'SSP10-'+out_name,
    ]#[0, 3, 5, 10, 25]

    # exps = ['0', '10']

    fig, axes = plt.subplots(1, 2, sharey=True, figsize=(8,5))

    filenames = []

    for exp in exps:
        subroot_dir = root_dir+str(beta)+'_'+exp+'_'+str(noise)+'_'+str(sparsity)

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

        for df in dfs:
            df.startTime = 1e-9 * (df.startTime - df.startTime.iloc[0])

        concatenated = pd.concat(dfs)
        concatenated.to_hdf('peak_data.h5', 'df_'+exp.replace('-', '_'))

        means = concatenated.mean(level=['iteration'])
        # means.time = means.time.cumsum()
        # means.startTime = 1e-9 * (means.startTime - means.startTime.iloc[0])
        errors = concatenated.std(level=['iteration'])

        # means.plot(ax=axes[0], y=column, yerr=errors, lw=2.0)
        means.plot(ax=axes[0], y=column, lw=1.5)
        # means.plot(ax=axes[1], x='time', y=column)
        means.plot(ax=axes[1], x='startTime', y=column, lw=1.5)

    for i in range(len(axes)):
        axes[i].legend(exps)
    axes[0].set_ylabel('Objective')
    axes[1].set_xlabel('Time (s)')

    plt.gcf().tight_layout()
    plt.savefig('peak'+'_'.join([column, out_name]) + '.eps')

if __name__ == '__main__':
    main(sys.argv[1:])
