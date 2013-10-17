class JoinError(Exception):

    def __init__(self, key=None, column=None, datafile=None):
        self.key = key
        self.column = column
        self.datafile = datafile

    def __str__(self):
        ret = ("Error in join: couldn't find key '%s' in column '%s' of %s" %
               (self.key, self.column, self.datafile))
        return ret


class DataFile:
    """
    A class for loading and manipulating basic text data files. Supports
    operations such as lookups, indexing, joins, and subsetting.
    """

    def __init__(self, fname, mode='r', sep=", ", *args, **kwargs):
        self.fd = open(fname, mode)
        self.dataframe = []
        self.__separator = sep
        self.fname = fname
        self.__have_read = False
        self.indices = dict()
        self.read(False, **kwargs)

    def __getstate__(self):
        return {
            'dataframe': self.dataframe,
            'sep': self.__separator,
            'fname': self.fname,
            'readstate': self.__have_read,
            'indices': self.indices
        }

    def __setstate__(self, state):
        self.dataframe = state['dataframe']
        self.__separator = state['sep']
        self.fname = state['fname']
        self.__have_read = state['readstate']
        self.indices = state['indices']

    def __getitem__(self, col):
        if col not in self.indices:
            raise KeyError("No index exists for '%s'" % col)

        return self.indices[col]

    def __str__(self):
        return "DataFile(%s)" % self.fname

    def add_index(self, column):
        if not self.__have_read:
            self.read()

        self.assert_column(column)

        if column in self.indices:
            return

        self.indices[column] = dict()
        for row in self.dataframe:
            try:
                self.indices[column][row[column]].append(row)
            except KeyError:
                self.indices[column][row[column]] = [row]

    def lookup(self, column, value, case_insensitive=False):
        if column not in self.indices:
            raise KeyError("No index exists for '%s'" % column)

        try:
            if case_insensitive:
                res = [self.indices[column][colval]
                       for colval in self.indices[column]
                       if value.lower() == colval.lower()]
                return res[0]  # you only get the first match

            return self.indices[column][value]
        except (KeyError, IndexError):
            return None

    def __len__(self):
        return len(self.dataframe)

    def subset(self, column, values):
        """
        Subset the dataset

        Remove all rows where the value in column is not
        in values, or where values (as a function) returns false.

        @param column: The name of the columns to subset based on.
        @type column: C{str} or C{list}
        @param values: A list of values that C{column} must be in
            or a function such that M{values(row[column])} returns
            false
        @type values: C{list} or function.
        """
        if isinstance(column, basestring):
            column = [column]

        if callable(values):
            filter_fun = lambda x: all([values(x[col]) for col in column])
        else:
            filter_fun = lambda x: all([x[col] in values for col in column])

        tmp = filter(filter_fun, self.dataframe)
        self.dataframe = tmp

    def truncate(self, column, values):
        """
        Truncate the dataset by removing all rows where the
        value in column(s) is in values.

        @param column: The name of the columns to subset based on.
        @type column: C{str} or C{list}
        @param values: A list of values that C{column} must be in
        @type values: C{list}
        """
        if isinstance(column, basestring):
            column = [column]

        tmp = filter(lambda x: all([x[col] not in values for col in column]),
                     self.dataframe)
        self.dataframe = tmp

    def maximum(self, column, cast=str):
        """
        Return the maximum value for the provided column.

        @param column: The name of the column heading to search
        @type column: C{str}
        @param cast: Optional type to cast the column values to
            before comparing.
        @type cast: C{str}, C{float}, or C{int}
        """
        if column not in self.dictkeys:
            raise KeyError("No column '%s' exists." % column)

        return max([cast(row[column]) for row in self.dataframe])

    def sorted(self, column, cast=str):
        """ Return a sorted copy of the dataset

        Sort the dataset on <b>column</b>. Cast the column to
        type <cast> before sorting.
        """
        if column not in self.dictkeys:
            raise KeyError("No column '%s' exists." % column)

        return sorted(self.dataframe, key=lambda row: cast(row[column]))

    def write(self, f, columns, subset=None, suppress_header=False):
        """
        Write the data to a file, specifying the names of columns
        to be printed.

        @param f: The filename to write to
        @type f: C{str}
        @param columns: The name of the columns to write out to the
            file, as either a string or a list.
        @type columns: C{str} or C{list}
        @keyword suppress_header: Don't include the column header
        """
        if not isinstance(columns, list):
            columns = [columns]

        try:
            if f.closed():
                raise IOError
        except (IOError, TypeError):
            f = open(f, 'w')

        if subset is None:
            toprint = self.dataframe
        else:
            toprint = subset

        for row in toprint:
            for column in columns:
                f.write("%s " % row[column])
            f.write("\n")

    def read(self, force=False, **kwargs):
        """
        Read the file associated with this instance and load the data
        into a usable representation. All lines beginning with '#' are
        ignored.

        @param force: Read even if have already read.
        @type force: C{bool}
        @keyword headers: A list of headers names to use. Must match the number
        of columns
        @keyword skiplines: Skip this many lines at the beginning
        """
        if self.__have_read and not force:
            return
        iterator = self.fd.__iter__()
        iter_ctr = 0
        if 'headers' in kwargs:
            self.dictkeys = kwargs['headers']
            iter_ctr += 1

        try:
            skiplines = kwargs['skiplines']
        except KeyError:
            skiplines = 0
        try:
            while True:
                line = iterator.next()
                if line[0] == '#':
                    continue
                if skiplines > 0:
                    skiplines -= 1
                    continue
                if iter_ctr == 0:
                    keys = line
                    self.dictkeys = [key.strip()
                                     for key in keys.split(self.__separator)]
                    iter_ctr += 1
                else:
                    iter_ctr += 1
                    linesep = line.split(self.__separator)
                    if len(linesep) != len(self.dictkeys):
                        raise IndexError("header length doesn't match number "
                                         "of columns")
                    self.dataframe.append(dict([(self.dictkeys[i], val.strip())
                                                for i, val
                                                in enumerate(linesep)]))
        except StopIteration:
            self.__have_read = True
            return iter_ctr

    def __iter__(self):
        if not self.__have_read:
            self.read()
        return self.dataframe.__iter__()

    def columns(self):
        return self.dictkeys

    def assert_column(self, name):
        if name not in self.columns():
            raise KeyError("Column {0} doesn't exist in dataset {1}. "
                           "Available columns: {2}\n"
                           .format(name, self, ", ".join(self.columns())))
        return True

    def join(self, dataset, add_columns, key_column):
        """
        Join several columns from one dataset to this one based on key
        columns. If C{dataset} has more than one row for a key value,
        the first one will be joined.

        @param dataset: The dataset to join to this one
        @type dataset: L{DataFile}
        @param add_columns: The names of columns to add to this dataset
            from C{dataset}
        @type add_columns: list
        @param key_column: A pair of identifiers for the key column in
            C{dataset} and C{self} respectively. Can be either column
            names or regexes. If regex' are given, the first column that
            matches will be used
        @type key_column: list
        @raise L{JoinError}: The requested rows could not be joined for
        some reason.
        """
#Convert regexes to column names if we can.
        try:
            for col in dataset.columns():
                res = key_column[0].match(col)
                if res and res.end() == len(col):
                    key_column[0] = col
        except AttributeError:
            pass  # It's fine, just leave it as a column name

        try:
            for col in self.columns():
                res = key_column[1].match(col)
                if res and res.end() == len(col):
                    key_column[1] = col
        except AttributeError:
            pass

        for column in add_columns:
            dataset.assert_column(column)
            self.dictkeys.append(column)
            dataset.add_index(column)

        dataset.add_index(key_column[0])
        self.add_index(key_column[1])

        for row in self:
            tomerge = dataset.lookup(
                key_column[0],
                row[key_column[1]],
                case_insensitive=isinstance(row[key_column[1]], str))

            if not tomerge:
                raise JoinError(key=row[key_column[1]],
                                column=key_column[0],
                                datafile=str(dataset))
            for column in add_columns:
                row[column] = tomerge[0][column]
