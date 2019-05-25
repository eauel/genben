from dask_mpi import initialize
initialize(interface='ib0')

if __name__ == '__main__':
    import genomics_benchmarks

    genomics_benchmarks.main()
