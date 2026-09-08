package bench

// LoadSuffix is exported to the test package so the naming rule that keeps a
// plain run's layout stable, and a sweep's filenames distinct, can be asserted
// directly rather than inferred from files on disk.
var LoadSuffix = loadSuffix
