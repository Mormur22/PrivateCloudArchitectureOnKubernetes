#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A wrapper class for Spark DataFrame to behave like pandas DataFrame.
"""
from collections import defaultdict, namedtuple
from collections.abc import Mapping
import re
import warnings
import inspect
import json
import types
from functools import partial, reduce
import sys
from itertools import zip_longest, chain
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    IO,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    no_type_check,
    TYPE_CHECKING,
)
import datetime

import numpy as np
import pandas as pd
from pandas.api.types import (  # type: ignore[attr-defined]
    is_bool_dtype,
    is_list_like,
    is_dict_like,
    is_scalar,
)
from pandas.tseries.frequencies import DateOffset, to_offset

if TYPE_CHECKING:
    from pandas.io.formats.style import Styler

from pandas.core.dtypes.common import infer_dtype_from_object
from pandas.core.accessor import CachedAccessor
from pandas.core.dtypes.inference import is_sequence
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame as SparkDataFrame, functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    NumericType,
    Row,
    StringType,
    StructField,
    StructType,
    DecimalType,
    TimestampType,
    TimestampNTZType,
)
from pyspark.sql.window import Window

from pyspark import pandas as ps  # For running doctests and reference resolution in PyCharm.
from pyspark.pandas._typing import Axis, DataFrameOrSeries, Dtype, Label, Name, Scalar, T
from pyspark.pandas.accessors import PandasOnSparkFrameMethods
from pyspark.pandas.config import option_context, get_option
from pyspark.pandas.correlation import (
    compute,
    CORRELATION_VALUE_1_COLUMN,
    CORRELATION_VALUE_2_COLUMN,
    CORRELATION_CORR_OUTPUT_COLUMN,
    CORRELATION_COUNT_OUTPUT_COLUMN,
)
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.spark.accessors import SparkFrameMethods, CachedSparkFrameMethods
from pyspark.pandas.utils import (
    align_diff_frames,
    column_labels_level,
    combine_frames,
    default_session,
    is_name_like_tuple,
    is_name_like_value,
    is_testing,
    name_like_string,
    same_anchor,
    scol_for,
    validate_arguments_and_invoke_function,
    validate_axis,
    validate_bool_kwarg,
    validate_how,
    validate_mode,
    verify_temp_column_name,
    log_advice,
)
from pyspark.pandas.generic import Frame
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    HIDDEN_COLUMNS,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_INDEX_NAME_FORMAT,
    SPARK_DEFAULT_INDEX_NAME,
    SPARK_DEFAULT_SERIES_NAME,
    SPARK_INDEX_NAME_PATTERN,
)
from pyspark.pandas.missing.frame import MissingPandasLikeDataFrame
from pyspark.pandas.typedef.typehints import (
    as_spark_type,
    infer_return_type,
    pandas_on_spark_type,
    spark_type_to_pandas_dtype,
    DataFrameType,
    SeriesType,
    ScalarType,
    create_tuple_for_frame_type,
)
from pyspark.pandas.plot import PandasOnSparkPlotAccessor

if TYPE_CHECKING:
    from pyspark.sql._typing import OptionalPrimitiveType

    from pyspark.pandas.groupby import DataFrameGroupBy
    from pyspark.pandas.resample import DataFrameResampler
    from pyspark.pandas.indexes import Index
    from pyspark.pandas.series import Series


# These regular expression patterns are compiled and defined here to avoid compiling the same
# pattern every time it is used in _repr_ and _repr_html_ in DataFrame.
# Two patterns basically seek the footer string from Pandas'
REPR_PATTERN = re.compile(r"\n\n\[(?P<rows>[0-9]+) rows x (?P<columns>[0-9]+) columns\]$")
REPR_HTML_PATTERN = re.compile(
    r"\n\<p\>(?P<rows>[0-9]+) rows × (?P<columns>[0-9]+) columns\<\/p\>\n\<\/div\>$"
)


_flex_doc_FRAME = """
Get {desc} of dataframe and other, element-wise (binary operator `{op_name}`).

Equivalent to ``{equiv}``. With the reverse version, `{reverse}`.

Among flexible wrappers (`add`, `sub`, `mul`, `div`) to
arithmetic operators: `+`, `-`, `*`, `/`, `//`.

Parameters
----------
other : scalar
    Any single data

Returns
-------
DataFrame
    Result of the arithmetic operation.

Examples
--------
>>> df = ps.DataFrame({{'angles': [0, 3, 4],
...                    'degrees': [360, 180, 360]}},
...                   index=['circle', 'triangle', 'rectangle'],
...                   columns=['angles', 'degrees'])
>>> df
           angles  degrees
circle          0      360
triangle        3      180
rectangle       4      360

Add a scalar with operator version which returns the same
results. Also, the reverse version.

>>> df + 1
           angles  degrees
circle          1      361
triangle        4      181
rectangle       5      361

>>> df.add(1)
           angles  degrees
circle          1      361
triangle        4      181
rectangle       5      361

>>> df.add(df)
           angles  degrees
circle          0      720
triangle        6      360
rectangle       8      720

>>> df + df + df
           angles  degrees
circle          0     1080
triangle        9      540
rectangle      12     1080

>>> df.radd(1)
           angles  degrees
circle          1      361
triangle        4      181
rectangle       5      361

Divide and true divide by constant with reverse version.

>>> df / 10
           angles  degrees
circle        0.0     36.0
triangle      0.3     18.0
rectangle     0.4     36.0

>>> df.div(10)
           angles  degrees
circle        0.0     36.0
triangle      0.3     18.0
rectangle     0.4     36.0

>>> df.rdiv(10)
             angles   degrees
circle          inf  0.027778
triangle   3.333333  0.055556
rectangle  2.500000  0.027778

>>> df.truediv(10)
           angles  degrees
circle        0.0     36.0
triangle      0.3     18.0
rectangle     0.4     36.0

>>> df.rtruediv(10)
             angles   degrees
circle          inf  0.027778
triangle   3.333333  0.055556
rectangle  2.500000  0.027778

Subtract by constant with reverse version.

>>> df - 1
           angles  degrees
circle         -1      359
triangle        2      179
rectangle       3      359

>>> df.sub(1)
           angles  degrees
circle         -1      359
triangle        2      179
rectangle       3      359

>>> df.rsub(1)
           angles  degrees
circle          1     -359
triangle       -2     -179
rectangle      -3     -359

Multiply by constant with the reverse version.

>>> df * 1
           angles  degrees
circle          0      360
triangle        3      180
rectangle       4      360

>>> df.mul(1)
           angles  degrees
circle          0      360
triangle        3      180
rectangle       4      360

>>> df.rmul(1)
           angles  degrees
circle          0      360
triangle        3      180
rectangle       4      360

Floor Divide by constant with reverse version.

>>> df // 10
           angles  degrees
circle        0.0     36.0
triangle      0.0     18.0
rectangle     0.0     36.0

>>> df.floordiv(10)
           angles  degrees
circle        0.0     36.0
triangle      0.0     18.0
rectangle     0.0     36.0

>>> df.rfloordiv(10)  # doctest: +SKIP
           angles  degrees
circle        inf      0.0
triangle      3.0      0.0
rectangle     2.0      0.0

Mod by constant with reverse version.

>>> df % 2
           angles  degrees
circle          0        0
triangle        1        0
rectangle       0        0

>>> df.mod(2)
           angles  degrees
circle          0        0
triangle        1        0
rectangle       0        0

>>> df.rmod(2)
           angles  degrees
circle        NaN        2
triangle      2.0        2
rectangle     2.0        2

Power by constant with reverse version.

>>> df ** 2
           angles   degrees
circle        0.0  129600.0
triangle      9.0   32400.0
rectangle    16.0  129600.0

>>> df.pow(2)
           angles   degrees
circle        0.0  129600.0
triangle      9.0   32400.0
rectangle    16.0  129600.0

>>> df.rpow(2)
           angles        degrees
circle        1.0  2.348543e+108
triangle      8.0   1.532496e+54
rectangle    16.0  2.348543e+108
"""


class DataFrame(Frame, Generic[T]):
    """
    pandas-on-Spark DataFrame that corresponds to pandas DataFrame logically. This holds Spark
    DataFrame internally.

    :ivar _internal: an internal immutable Frame to manage metadata.
    :type _internal: InternalFrame

    Parameters
    ----------
    data : numpy ndarray (structured or homogeneous), dict, pandas DataFrame,
        Spark DataFrame, pandas-on-Spark DataFrame or pandas-on-Spark Series.
        Dict can contain Series, arrays, constants, or list-like objects
    index : Index or array-like
        Index to use for the resulting frame. Will default to RangeIndex if
        no indexing information part of input data and no index provided
    columns : Index or array-like
        Column labels to use for the resulting frame. Will default to
        RangeIndex (0, 1, 2, ..., n) if no column labels are provided
    dtype : dtype, default None
        Data type to force. Only a single dtype is allowed. If None, infer
    copy : boolean, default False
        Copy data from inputs. Only affects DataFrame / 2d ndarray input

    .. versionchanged:: 3.4.0
        Since 3.4.0, it deals with `data` and `index` in this approach:
        1, when `data` is a distributed dataset (Internal DataFrame/Spark DataFrame/
        pandas-on-Spark DataFrame/pandas-on-Spark Series), it will first parallelize
        the `index` if necessary, and then try to combine the `data` and `index`;
        Note that if `data` and `index` doesn't have the same anchor, then
        `compute.ops_on_diff_frames` should be turned on;
        2, when `data` is a local dataset (Pandas DataFrame/numpy ndarray/list/etc),
        it will first collect the `index` to driver if necessary, and then apply
        the `Pandas.DataFrame(...)` creation internally;

    Examples
    --------
    Constructing DataFrame from a dictionary.

    >>> d = {'col1': [1, 2], 'col2': [3, 4]}
    >>> df = ps.DataFrame(data=d, columns=['col1', 'col2'])
    >>> df
       col1  col2
    0     1     3
    1     2     4

    Constructing DataFrame from pandas DataFrame

    >>> df = ps.DataFrame(pd.DataFrame(data=d, columns=['col1', 'col2']))
    >>> df
       col1  col2
    0     1     3
    1     2     4

    Notice that the inferred dtype is int64.

    >>> df.dtypes
    col1    int64
    col2    int64
    dtype: object

    To enforce a single dtype:

    >>> df = ps.DataFrame(data=d, dtype=np.int8)
    >>> df.dtypes
    col1    int8
    col2    int8
    dtype: object

    Constructing DataFrame from numpy ndarray:

    >>> import numpy as np
    >>> ps.DataFrame(data=np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]),
    ...     columns=['a', 'b', 'c', 'd', 'e'])
       a  b  c  d  e
    0  1  2  3  4  5
    1  6  7  8  9  0

    Constructing DataFrame from numpy ndarray with Pandas index:

    >>> import numpy as np
    >>> import pandas as pd

    >>> ps.DataFrame(data=np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]),
    ...     index=pd.Index([1, 4]), columns=['a', 'b', 'c', 'd', 'e'])
       a  b  c  d  e
    1  1  2  3  4  5
    4  6  7  8  9  0

    Constructing DataFrame from numpy ndarray with pandas-on-Spark index:

    >>> import numpy as np
    >>> import pandas as pd
    >>> ps.DataFrame(data=np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]),
    ...     index=ps.Index([1, 4]), columns=['a', 'b', 'c', 'd', 'e'])
       a  b  c  d  e
    1  1  2  3  4  5
    4  6  7  8  9  0

    Constructing DataFrame from Pandas DataFrame with Pandas index:

    >>> import numpy as np
    >>> import pandas as pd
    >>> pdf = pd.DataFrame(data=np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]),
    ...     columns=['a', 'b', 'c', 'd', 'e'])
    >>> ps.DataFrame(data=pdf, index=pd.Index([1, 4]))
         a    b    c    d    e
    1  6.0  7.0  8.0  9.0  0.0
    4  NaN  NaN  NaN  NaN  NaN

    Constructing DataFrame from Pandas DataFrame with pandas-on-Spark index:

    >>> import numpy as np
    >>> import pandas as pd
    >>> pdf = pd.DataFrame(data=np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]),
    ...     columns=['a', 'b', 'c', 'd', 'e'])
    >>> ps.DataFrame(data=pdf, index=ps.Index([1, 4]))
         a    b    c    d    e
    1  6.0  7.0  8.0  9.0  0.0
    4  NaN  NaN  NaN  NaN  NaN

    Constructing DataFrame from Spark DataFrame with Pandas index:

    >>> import pandas as pd
    >>> sdf = spark.createDataFrame([("Data", 1), ("Bricks", 2)], ["x", "y"])
    >>> ps.DataFrame(data=sdf, index=pd.Index([0, 1, 2]))
    Traceback (most recent call last):
      ...
    ValueError: Cannot combine the series or dataframe...'compute.ops_on_diff_frames' option.

    Enable 'compute.ops_on_diff_frames' to combine SparkDataFrame and Pandas index

    >>> with ps.option_context("compute.ops_on_diff_frames", True):
    ...     ps.DataFrame(data=sdf, index=pd.Index([0, 1, 2]))
            x    y
    0    Data  1.0
    1  Bricks  2.0
    2    None  NaN

    Constructing DataFrame from Spark DataFrame with pandas-on-Spark index:

    >>> import pandas as pd
    >>> sdf = spark.createDataFrame([("Data", 1), ("Bricks", 2)], ["x", "y"])
    >>> ps.DataFrame(data=sdf, index=ps.Index([0, 1, 2]))
    Traceback (most recent call last):
      ...
    ValueError: Cannot combine the series or dataframe...'compute.ops_on_diff_frames' option.

    Enable 'compute.ops_on_diff_frames' to combine Spark DataFrame and pandas-on-Spark index

    >>> with ps.option_context("compute.ops_on_diff_frames", True):
    ...     ps.DataFrame(data=sdf, index=ps.Index([0, 1, 2]))
            x    y
    0    Data  1.0
    1  Bricks  2.0
    2    None  NaN
    """

    def __init__(  # type: ignore[no-untyped-def]
        self, data=None, index=None, columns=None, dtype=None, copy=False
    ):
        index_assigned = False
        if isinstance(data, InternalFrame):
            assert columns is None
            assert dtype is None
            assert not copy
            if index is None:
                internal = data
        elif isinstance(data, SparkDataFrame):
            assert columns is None
            assert dtype is None
            assert not copy
            if index is None:
                internal = InternalFrame(spark_frame=data, index_spark_columns=None)
        elif isinstance(data, ps.DataFrame):
            assert columns is None
            assert dtype is None
            assert not copy
            if index is None:
                internal = data._internal
        elif isinstance(data, ps.Series):
            assert dtype is None
            assert not copy
            # For pandas compatibility when `columns` contains only one valid column.
            if columns is not None:
                assert isinstance(columns, (dict, list, tuple))
                assert len(columns) == 1
                columns = list(columns.keys()) if isinstance(columns, dict) else columns
                assert columns[0] == data._internal.data_spark_column_names[0]
            if index is None:
                internal = data.to_frame()._internal
        else:
            from pyspark.pandas.indexes.base import Index

            if index is not None and isinstance(index, Index):
                # with local data, collect ps.Index to driver
                # to avoid mismatched results between
                # ps.DataFrame([1, 2], index=ps.Index([1, 2]))
                # and
                # pd.DataFrame([1, 2], index=pd.Index([1, 2]))
                index = index._to_pandas()

            pdf = pd.DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
            internal = InternalFrame.from_pandas(pdf)
            index_assigned = True

        if index is not None and not index_assigned:
            # TODO(SPARK-40226): Support MultiIndex
            if isinstance(index, (ps.MultiIndex, pd.MultiIndex)):
                raise ValueError("Cannot combine a Distributed Dataset with a MultiIndex")

            data_df = ps.DataFrame(data=data, index=None, columns=columns, dtype=dtype, copy=copy)
            index_ps = ps.Index(index)
            index_df = index_ps.to_frame()

            if same_anchor(data_df, index_df):
                data_labels = data_df._internal.column_labels
                data_pssers = [data_df._psser_for(label) for label in data_labels]
                index_labels = index_df._internal.column_labels
                index_pssers = [index_df._psser_for(label) for label in index_labels]
                internal = data_df._internal.with_new_columns(data_pssers + index_pssers)

                combined = ps.DataFrame(internal).set_index(index_labels)
                combined.index.name = index_ps.name
            else:
                # drop un-matched rows in `data`
                # note that `combine_frames` cannot work with a MultiIndex for now
                combined = combine_frames(data_df, index_df, how="right")
                combined_labels = combined._internal.column_labels
                index_labels = [label for label in combined_labels if label[0] == "that"]
                combined = combined.set_index(index_labels)

                combined._internal._column_labels = data_df._internal.column_labels
                combined._internal._column_label_names = data_df._internal._column_label_names
                combined._internal._index_names = index_df._internal.column_labels
                combined.index.name = index_ps.name

            internal = combined._internal

        object.__setattr__(self, "_internal_frame", internal)

    @property
    def _pssers(self) -> Dict[Label, "Series"]:
        """Return a dict of column label -> Series which anchors `self`."""
        from pyspark.pandas.series import Series

        if not hasattr(self, "_psseries"):
            object.__setattr__(
                self,
                "_psseries",
                {label: Series(data=self, index=label) for label in self._internal.column_labels},
            )
        else:
            psseries = cast(Dict[Label, Series], self._psseries)  # type: ignore[has-type]
            assert len(self._internal.column_labels) == len(psseries), (
                len(self._internal.column_labels),
                len(psseries),
            )
            if any(self is not psser._psdf for psser in psseries.values()):
                # Refresh the dict to contain only Series anchoring `self`.
                self._psseries = {
                    label: (
                        psseries[label]
                        if self is psseries[label]._psdf
                        else Series(data=self, index=label)
                    )
                    for label in self._internal.column_labels
                }
        return self._psseries

    @property
    def _internal(self) -> InternalFrame:
        return cast(InternalFrame, self._internal_frame)  # type: ignore[has-type]

    def _update_internal_frame(
        self,
        internal: InternalFrame,
        check_same_anchor: bool = True,
        anchor_force_disconnect: bool = False,
    ) -> None:
        """
        Update InternalFrame with the given one.

        If the column_label is changed or the new InternalFrame is not the same `anchor` or the
        `anchor_force_disconnect` flag is set to True, disconnect the original anchor and create
        a new one.

        If `check_same_anchor` is `False`, checking whether the same anchor is ignored
        and force to update the InternalFrame, e.g., replacing the internal with the resolved_copy,
        updating the underlying Spark DataFrame which need to combine a different Spark DataFrame.

        Parameters
        ----------
        internal : InternalFrame
            The new InternalFrame
        check_same_anchor : bool
            Whether checking the same anchor
        anchor_force_disconnect : bool
            Force to disconnect the original anchor and create a new one
        """
        from pyspark.pandas.series import Series

        if hasattr(self, "_psseries"):
            psseries = {}

            for old_label, new_label in zip_longest(
                self._internal.column_labels, internal.column_labels
            ):
                if old_label is not None:
                    psser = self._pssers[old_label]

                    renamed = old_label != new_label
                    not_same_anchor = check_same_anchor and not same_anchor(internal, psser)

                    if renamed or not_same_anchor or anchor_force_disconnect:
                        psdf: DataFrame = DataFrame(self._internal.select_column(old_label))
                        psser._update_anchor(psdf)
                        psser = None
                else:
                    psser = None
                if new_label is not None:
                    if psser is None:
                        psser = Series(data=self, index=new_label)
                    psseries[new_label] = psser

            self._psseries = psseries

        self._internal_frame = internal

        if hasattr(self, "_repr_pandas_cache"):
            del self._repr_pandas_cache

    @property
    def ndim(self) -> int:
        """
        Return an int representing the number of array dimensions.

        return 2 for DataFrame.

        Examples
        --------

        >>> df = ps.DataFrame([[1, 2], [4, 5], [7, 8]],
        ...                   index=['cobra', 'viper', None],
        ...                   columns=['max_speed', 'shield'])
        >>> df  # doctest: +SKIP
               max_speed  shield
        cobra          1       2
        viper          4       5
        None           7       8
        >>> df.ndim
        2
        """
        return 2

    @property
    def axes(self) -> List:
        """
        Return a list representing the axes of the DataFrame.

        It has the row axis labels and column axis labels as the only members.
        They are returned in that order.

        Examples
        --------

        >>> df = ps.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> df.axes
        [Int64Index([0, 1], dtype='int64'), Index(['col1', 'col2'], dtype='object')]
        """
        return [self.index, self.columns]

    def _reduce_for_stat_function(
        self,
        sfun: Callable[["Series"], Column],
        name: str,
        axis: Optional[Axis] = None,
        numeric_only: bool = True,
        skipna: bool = True,
        **kwargs: Any,
    ) -> "Series":
        """
        Applies sfun to each column and returns a pd.Series where the number of rows equals the
        number of columns.

        Parameters
        ----------
        sfun : either an 1-arg function that takes a Column and returns a Column, or
            a 2-arg function that takes a Column and its DataType and returns a Column.
            axis: used only for sanity check because the series only supports index axis.
        name : original pandas API name.
        axis : axis to apply. 0 or 1, or 'index' or 'columns.
        numeric_only : bool, default True
            Include only float, int, boolean columns. False is not supported. This parameter
            is mainly for pandas compatibility. Only 'DataFrame.count' uses this parameter
            currently.
        skipna : bool, default True
            Exclude NA/null values when computing the result.
        """
        from pyspark.pandas.series import Series, first_series

        axis = validate_axis(axis)
        if axis == 0:
            min_count = kwargs.get("min_count", 0)

            exprs = [F.lit(None).cast(StringType()).alias(SPARK_DEFAULT_INDEX_NAME)]
            new_column_labels = []
            for label in self._internal.column_labels:
                psser = self._psser_for(label)

                is_numeric_or_boolean = isinstance(
                    psser.spark.data_type, (NumericType, BooleanType)
                )
                keep_column = not numeric_only or is_numeric_or_boolean

                if keep_column:
                    if not skipna and get_option("compute.eager_check") and psser.hasnans:
                        scol = F.first(F.lit(np.nan))
                    else:
                        scol = sfun(psser)

                    if min_count > 0:
                        scol = F.when(Frame._count_expr(psser) >= min_count, scol)

                    exprs.append(scol.alias(name_like_string(label)))
                    new_column_labels.append(label)

            if len(exprs) == 1:
                return Series([])

            sdf = self._internal.spark_frame.select(*exprs)

            # The data is expected to be small so it's fine to transpose/use the default index.
            with ps.option_context("compute.max_rows", 1):
                internal = InternalFrame(
                    spark_frame=sdf,
                    index_spark_columns=[scol_for(sdf, SPARK_DEFAULT_INDEX_NAME)],
                    column_labels=new_column_labels,
                    column_label_names=self._internal.column_label_names,
                )
                return first_series(DataFrame(internal).transpose())

        else:
            # Here we execute with the first 1000 to get the return type.
            # If the records were less than 1000, it uses pandas API directly for a shortcut.
            limit = get_option("compute.shortcut_limit")
            pdf = self.head(limit + 1)._to_internal_pandas()
            pser = getattr(pdf, name)(axis=axis, numeric_only=numeric_only, **kwargs)
            if len(pdf) <= limit:
                return Series(pser)

            @pandas_udf(returnType=as_spark_type(pser.dtype.type))  # type: ignore[call-overload]
            def calculate_columns_axis(*cols: pd.Series) -> pd.Series:
                return getattr(pd.concat(cols, axis=1), name)(
                    axis=axis, numeric_only=numeric_only, **kwargs
                )

            column_name = verify_temp_column_name(
                self._internal.spark_frame.select(self._internal.index_spark_columns),
                "__calculate_columns_axis__",
            )
            sdf = self._internal.spark_frame.select(
                self._internal.index_spark_columns
                + [calculate_columns_axis(*self._internal.data_spark_columns).alias(column_name)]
            )
            internal = InternalFrame(
                spark_frame=sdf,
                index_spark_columns=[
                    scol_for(sdf, col) for col in self._internal.index_spark_column_names
                ],
                index_names=self._internal.index_names,
                index_fields=self._internal.index_fields,
            )
            return first_series(DataFrame(internal)).rename(pser.name)

    def _psser_for(self, label: Label) -> "Series":
        """
        Create Series with a proper column label.

        The given label must be verified to exist in `InternalFrame.column_labels`.

        For example, in some method, self is like:

        >>> self = ps.range(3)

        `self._psser_for(label)` can be used with `InternalFrame.column_labels`:

        >>> self._psser_for(self._internal.column_labels[0])
        0    0
        1    1
        2    2
        Name: id, dtype: int64

        `self._psser_for(label)` must not be used directly with user inputs.
        In that case, `self[label]` should be used instead, which checks the label exists or not:

        >>> self['id']
        0    0
        1    1
        2    2
        Name: id, dtype: int64
        """
        return self._pssers[label]

    def _apply_series_op(
        self, op: Callable[["Series"], Union["Series", Column]], should_resolve: bool = False
    ) -> "DataFrame":
        applied = []
        for label in self._internal.column_labels:
            applied.append(op(self._psser_for(label)))
        internal = self._internal.with_new_columns(applied)
        if should_resolve:
            internal = internal.resolved_copy
        return DataFrame(internal)

    # Arithmetic Operators
    def _map_series_op(self, op: str, other: Any) -> "DataFrame":
        from pyspark.pandas.base import IndexOpsMixin

        if not isinstance(other, DataFrame) and (
            isinstance(other, IndexOpsMixin) or is_sequence(other)
        ):
            raise TypeError(
                "%s with a sequence is currently not supported; "
                "however, got %s." % (op, type(other).__name__)
            )

        if isinstance(other, DataFrame):
            if self._internal.column_labels_level != other._internal.column_labels_level:
                raise ValueError("cannot join with no overlapping index names")

            if not same_anchor(self, other):
                # Different DataFrames
                def apply_op(
                    psdf: DataFrame,
                    this_column_labels: List[Label],
                    that_column_labels: List[Label],
                ) -> Iterator[Tuple["Series", Label]]:
                    for this_label, that_label in zip(this_column_labels, that_column_labels):
                        yield (
                            getattr(psdf._psser_for(this_label), op)(
                                psdf._psser_for(that_label)
                            ).rename(this_label),
                            this_label,
                        )

                return align_diff_frames(apply_op, self, other, fillna=True, how="full")
            else:
                applied = []
                column_labels = []
                for label in self._internal.column_labels:
                    if label in other._internal.column_labels:
                        applied.append(getattr(self._psser_for(label), op)(other._psser_for(label)))
                    else:
                        applied.append(
                            F.lit(None)
                            .cast(self._internal.spark_type_for(label))
                            .alias(name_like_string(label))
                        )
                    column_labels.append(label)
                for label in other._internal.column_labels:
                    if label not in column_labels:
                        applied.append(
                            F.lit(None)
                            .cast(other._internal.spark_type_for(label))
                            .alias(name_like_string(label))
                        )
                        column_labels.append(label)
                internal = self._internal.with_new_columns(applied, column_labels=column_labels)
                return DataFrame(internal)
        else:
            return self._apply_series_op(lambda psser: getattr(psser, op)(other))

    def __add__(self, other: Any) -> "DataFrame":
        return self._map_series_op("add", other)

    def __radd__(self, other: Any) -> "DataFrame":
        return self._map_series_op("radd", other)

    def __truediv__(self, other: Any) -> "DataFrame":
        return self._map_series_op("truediv", other)

    def __rtruediv__(self, other: Any) -> "DataFrame":
        return self._map_series_op("rtruediv", other)

    def __mul__(self, other: Any) -> "DataFrame":
        return self._map_series_op("mul", other)

    def __rmul__(self, other: Any) -> "DataFrame":
        return self._map_series_op("rmul", other)

    def __sub__(self, other: Any) -> "DataFrame":
        return self._map_series_op("sub", other)

    def __rsub__(self, other: Any) -> "DataFrame":
        return self._map_series_op("rsub", other)

    def __pow__(self, other: Any) -> "DataFrame":
        return self._map_series_op("pow", other)

    def __rpow__(self, other: Any) -> "DataFrame":
        return self._map_series_op("rpow", other)

    def __mod__(self, other: Any) -> "DataFrame":
        return self._map_series_op("mod", other)

    def __rmod__(self, other: Any) -> "DataFrame":
        return self._map_series_op("rmod", other)

    def __floordiv__(self, other: Any) -> "DataFrame":
        return self._map_series_op("floordiv", other)

    def __rfloordiv__(self, other: Any) -> "DataFrame":
        return self._map_series_op("rfloordiv", other)

    def __abs__(self) -> "DataFrame":
        return self._apply_series_op(lambda psser: abs(psser))

    def __neg__(self) -> "DataFrame":
        return self._apply_series_op(lambda psser: -psser)

    def add(self, other: Any) -> "DataFrame":
        return self + other

    # create accessor for plot
    plot = CachedAccessor("plot", PandasOnSparkPlotAccessor)

    # create accessor for Spark related methods.
    spark = CachedAccessor("spark", SparkFrameMethods)

    # create accessor for pandas-on-Spark specific methods.
    pandas_on_spark = CachedAccessor("pandas_on_spark", PandasOnSparkFrameMethods)

    # keep the name "koalas" for backward compatibility.
    koalas = CachedAccessor("koalas", PandasOnSparkFrameMethods)

    @no_type_check
    def hist(self, bins=10, **kwds):
        return self.plot.hist(bins, **kwds)

    hist.__doc__ = PandasOnSparkPlotAccessor.hist.__doc__

    @no_type_check
    def boxplot(self, **kwds):
        return self.plot.box(**kwds)

    boxplot.__doc__ = PandasOnSparkPlotAccessor.box.__doc__

    @no_type_check
    def kde(self, bw_method=None, ind=None, **kwds):
        return self.plot.kde(bw_method, ind, **kwds)

    kde.__doc__ = PandasOnSparkPlotAccessor.kde.__doc__

    add.__doc__ = _flex_doc_FRAME.format(
        desc="Addition", op_name="+", equiv="dataframe + other", reverse="radd"
    )

    def radd(self, other: Any) -> "DataFrame":
        return other + self

    radd.__doc__ = _flex_doc_FRAME.format(
        desc="Addition", op_name="+", equiv="other + dataframe", reverse="add"
    )

    def div(self, other: Any) -> "DataFrame":
        return self / other

    div.__doc__ = _flex_doc_FRAME.format(
        desc="Floating division", op_name="/", equiv="dataframe / other", reverse="rdiv"
    )

    divide = div

    def rdiv(self, other: Any) -> "DataFrame":
        return other / self

    rdiv.__doc__ = _flex_doc_FRAME.format(
        desc="Floating division", op_name="/", equiv="other / dataframe", reverse="div"
    )

    def truediv(self, other: Any) -> "DataFrame":
        return self / other

    truediv.__doc__ = _flex_doc_FRAME.format(
        desc="Floating division", op_name="/", equiv="dataframe / other", reverse="rtruediv"
    )

    def rtruediv(self, other: Any) -> "DataFrame":
        return other / self

    rtruediv.__doc__ = _flex_doc_FRAME.format(
        desc="Floating division", op_name="/", equiv="other / dataframe", reverse="truediv"
    )

    def mul(self, other: Any) -> "DataFrame":
        return self * other

    mul.__doc__ = _flex_doc_FRAME.format(
        desc="Multiplication", op_name="*", equiv="dataframe * other", reverse="rmul"
    )

    multiply = mul

    def rmul(self, other: Any) -> "DataFrame":
        return other * self

    rmul.__doc__ = _flex_doc_FRAME.format(
        desc="Multiplication", op_name="*", equiv="other * dataframe", reverse="mul"
    )

    def sub(self, other: Any) -> "DataFrame":
        return self - other

    sub.__doc__ = _flex_doc_FRAME.format(
        desc="Subtraction", op_name="-", equiv="dataframe - other", reverse="rsub"
    )

    subtract = sub

    def rsub(self, other: Any) -> "DataFrame":
        return other - self

    rsub.__doc__ = _flex_doc_FRAME.format(
        desc="Subtraction", op_name="-", equiv="other - dataframe", reverse="sub"
    )

    def mod(self, other: Any) -> "DataFrame":
        return self % other

    mod.__doc__ = _flex_doc_FRAME.format(
        desc="Modulo", op_name="%", equiv="dataframe % other", reverse="rmod"
    )

    def rmod(self, other: Any) -> "DataFrame":
        return other % self

    rmod.__doc__ = _flex_doc_FRAME.format(
        desc="Modulo", op_name="%", equiv="other % dataframe", reverse="mod"
    )

    def pow(self, other: Any) -> "DataFrame":
        return self**other

    pow.__doc__ = _flex_doc_FRAME.format(
        desc="Exponential power of series", op_name="**", equiv="dataframe ** other", reverse="rpow"
    )

    def rpow(self, other: Any) -> "DataFrame":
        return other**self

    rpow.__doc__ = _flex_doc_FRAME.format(
        desc="Exponential power", op_name="**", equiv="other ** dataframe", reverse="pow"
    )

    def floordiv(self, other: Any) -> "DataFrame":
        return self // other

    floordiv.__doc__ = _flex_doc_FRAME.format(
        desc="Integer division", op_name="//", equiv="dataframe // other", reverse="rfloordiv"
    )

    def rfloordiv(self, other: Any) -> "DataFrame":
        return other // self

    rfloordiv.__doc__ = _flex_doc_FRAME.format(
        desc="Integer division", op_name="//", equiv="other // dataframe", reverse="floordiv"
    )

    # Comparison Operators
    def __eq__(self, other: Any) -> "DataFrame":  # type: ignore[override]
        return self._map_series_op("eq", other)

    def __ne__(self, other: Any) -> "DataFrame":  # type: ignore[override]
        return self._map_series_op("ne", other)

    def __lt__(self, other: Any) -> "DataFrame":
        return self._map_series_op("lt", other)

    def __le__(self, other: Any) -> "DataFrame":
        return self._map_series_op("le", other)

    def __ge__(self, other: Any) -> "DataFrame":
        return self._map_series_op("ge", other)

    def __gt__(self, other: Any) -> "DataFrame":
        return self._map_series_op("gt", other)

    def eq(self, other: Any) -> "DataFrame":
        """
        Compare if the current value is equal to the other.

        >>> df = ps.DataFrame({'a': [1, 2, 3, 4],
        ...                    'b': [1, np.nan, 1, np.nan]},
        ...                   index=['a', 'b', 'c', 'd'], columns=['a', 'b'])

        >>> df.eq(1)
               a      b
        a   True   True
        b  False  False
        c  False   True
        d  False  False
        """
        return self == other

    equals = eq

    def gt(self, other: Any) -> "DataFrame":
        """
        Compare if the current value is greater than the other.

        >>> df = ps.DataFrame({'a': [1, 2, 3, 4],
        ...                    'b': [1, np.nan, 1, np.nan]},
        ...                   index=['a', 'b', 'c', 'd'], columns=['a', 'b'])

        >>> df.gt(2)
               a      b
        a  False  False
        b  False  False
        c   True  False
        d   True  False
        """
        return self > other

    def ge(self, other: Any) -> "DataFrame":
        """
        Compare if the current value is greater than or equal to the other.

        >>> df = ps.DataFrame({'a': [1, 2, 3, 4],
        ...                    'b': [1, np.nan, 1, np.nan]},
        ...                   index=['a', 'b', 'c', 'd'], columns=['a', 'b'])

        >>> df.ge(1)
              a      b
        a  True   True
        b  True  False
        c  True   True
        d  True  False
        """
        return self >= other

    def lt(self, other: Any) -> "DataFrame":
        """
        Compare if the current value is less than the other.

        >>> df = ps.DataFrame({'a': [1, 2, 3, 4],
        ...                    'b': [1, np.nan, 1, np.nan]},
        ...                   index=['a', 'b', 'c', 'd'], columns=['a', 'b'])

        >>> df.lt(1)
               a      b
        a  False  False
        b  False  False
        c  False  False
        d  False  False
        """
        return self < other

    def le(self, other: Any) -> "DataFrame":
        """
        Compare if the current value is less than or equal to the other.

        >>> df = ps.DataFrame({'a': [1, 2, 3, 4],
        ...                    'b': [1, np.nan, 1, np.nan]},
        ...                   index=['a', 'b', 'c', 'd'], columns=['a', 'b'])

        >>> df.le(2)
               a      b
        a   True   True
        b   True  False
        c  False   True
        d  False  False
        """
        return self <= other

    def ne(self, other: Any) -> "DataFrame":
        """
        Compare if the current value is not equal to the other.

        >>> df = ps.DataFrame({'a': [1, 2, 3, 4],
        ...                    'b': [1, np.nan, 1, np.nan]},
        ...                   index=['a', 'b', 'c', 'd'], columns=['a', 'b'])

        >>> df.ne(1)
               a      b
        a  False  False
        b   True   True
        c   True  False
        d   True   True
        """
        return self != other

    def applymap(self, func: Callable[[Any], Any]) -> "DataFrame":
        """
        Apply a function to a Dataframe elementwise.

        This method applies a function that accepts and returns a scalar
        to every element of a DataFrame.

        .. note:: this API executes the function once to infer the type which is
             potentially expensive, for instance, when the dataset is created after
             aggregations or sorting.

             To avoid this, specify return type in ``func``, for instance, as below:

             >>> def square(x) -> np.int32:
             ...     return x ** 2

             pandas-on-Spark uses return type hints and does not try to infer the type.

        Parameters
        ----------
        func : callable
            Python function returns a single value from a single value.

        Returns
        -------
        DataFrame
            Transformed DataFrame.

        Examples
        --------
        >>> df = ps.DataFrame([[1, 2.12], [3.356, 4.567]])
        >>> df
               0      1
        0  1.000  2.120
        1  3.356  4.567

        >>> def str_len(x) -> int:
        ...     return len(str(x))
        >>> df.applymap(str_len)
           0  1
        0  3  4
        1  5  5

        >>> def power(x) -> float:
        ...     return x ** 2
        >>> df.applymap(power)
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489

        You can omit type hints and let pandas-on-Spark infer its type.

        >>> df.applymap(lambda x: x ** 2)
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489
        """

        # TODO: We can implement shortcut theoretically since it creates new DataFrame
        #  anyway and we don't have to worry about operations on different DataFrames.
        return self._apply_series_op(lambda psser: psser.apply(func))

    # TODO: not all arguments are implemented comparing to pandas' for now.
    def aggregate(self, func: Union[List[str], Dict[Name, List[str]]]) -> "DataFrame":
        """Aggregate using one or more operations over the specified axis.

        Parameters
        ----------
        func : dict or a list
             a dict mapping from column name (string) to
             aggregate functions (list of strings).
             If a list is given, the aggregation is performed against
             all columns.

        Returns
        -------
        DataFrame

        Notes
        -----
        `agg` is an alias for `aggregate`. Use the alias.

        See Also
        --------
        DataFrame.apply : Invoke function on DataFrame.
        DataFrame.transform : Only perform transforming type operations.
        DataFrame.groupby : Perform operations over groups.
        Series.aggregate : The equivalent function for Series.

        Examples
        --------
        >>> df = ps.DataFrame([[1, 2, 3],
        ...                    [4, 5, 6],
        ...                    [7, 8, 9],
        ...                    [np.nan, np.nan, np.nan]],
        ...                   columns=['A', 'B', 'C'])

        >>> df
             A    B    C
        0  1.0  2.0  3.0
        1  4.0  5.0  6.0
        2  7.0  8.0  9.0
        3  NaN  NaN  NaN

        Aggregate these functions over the rows.

        >>> df.agg(['sum', 'min'])[['A', 'B', 'C']].sort_index()
                A     B     C
        min   1.0   2.0   3.0
        sum  12.0  15.0  18.0

        Different aggregations per column.

        >>> df.agg({'A' : ['sum', 'min'], 'B' : ['min', 'max']})[['A', 'B']].sort_index()
                A    B
        max   NaN  8.0
        min   1.0  2.0
        sum  12.0  NaN

        For multi-index columns:

        >>> df.columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        >>> df.agg(['sum', 'min'])[[("X", "A"), ("X", "B"), ("Y", "C")]].sort_index()
                X           Y
                A     B     C
        min   1.0   2.0   3.0
        sum  12.0  15.0  18.0

        >>> aggregated = df.agg({("X", "A") : ['sum', 'min'], ("X", "B") : ['min', 'max']})
        >>> aggregated[[("X", "A"), ("X", "B")]].sort_index()  # doctest: +NORMALIZE_WHITESPACE
                X
                A    B
        max   NaN  8.0
        min   1.0  2.0
        sum  12.0  NaN
        """
        from pyspark.pandas.groupby import GroupBy

        if isinstance(func, list):
            if all((isinstance(f, str) for f in func)):
                func = dict([(column, func) for column in self.columns])
            else:
                raise ValueError(
                    "If the given function is a list, it "
                    "should only contains function names as strings."
                )

        if not isinstance(func, dict) or not all(
            is_name_like_value(key)
            and (
                isinstance(value, str)
                or (isinstance(value, list) and all(isinstance(v, str) for v in value))
            )
            for key, value in func.items()
        ):
            raise ValueError(
                "aggs must be a dict mapping from column name to aggregate "
                "functions (string or list of strings)."
            )

        with option_context("compute.default_index_type", "distributed"):
            psdf: DataFrame = DataFrame(GroupBy._spark_groupby(self, func))

            # The codes below basically convert:
            #
            #           A         B
            #         sum  min  min  max
            #     0  12.0  1.0  2.0  8.0
            #
            # to:
            #             A    B
            #     max   NaN  8.0
            #     min   1.0  2.0
            #     sum  12.0  NaN
            #
            # Aggregated output is usually pretty much small.

            return psdf.stack().droplevel(0)[list(func.keys())]

    agg = aggregate

    def corr(self, method: str = "pearson", min_periods: Optional[int] = None) -> "DataFrame":
        """
        Compute pairwise correlation of columns, excluding NA/null values.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        method : {'pearson', 'spearman', 'kendall'}
            * pearson : standard correlation coefficient
            * spearman : Spearman rank correlation
            * kendall : Kendall Tau correlation coefficient

            .. versionchanged:: 3.4.0
               support 'kendall' for method parameter
        min_periods : int, optional
            Minimum number of observations required per pair of columns
            to have a valid result.

            .. versionadded:: 3.4.0

        Returns
        -------
        DataFrame

        See Also
        --------
        DataFrame.corrwith
        Series.corr

        Notes
        -----
        1. Pearson, Kendall and Spearman correlation are currently computed using pairwise
           complete observations.

        2. The complexity of Kendall correlation is O(#row * #row), if the dataset is too
           large, sampling ahead of correlation computation is recommended.

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'])
        >>> df.corr('pearson')
                  dogs      cats
        dogs  1.000000 -0.851064
        cats -0.851064  1.000000

        >>> df.corr('spearman')
                  dogs      cats
        dogs  1.000000 -0.948683
        cats -0.948683  1.000000

        >>> df.corr('kendall')
                  dogs      cats
        dogs  1.000000 -0.912871
        cats -0.912871  1.000000
        """
        if method not in ["pearson", "spearman", "kendall"]:
            raise ValueError(f"Invalid method {method}")
        if min_periods is not None and not isinstance(min_periods, int):
            raise TypeError(f"Invalid min_periods type {type(min_periods).__name__}")

        min_periods = 1 if min_periods is None else min_periods
        internal = self._internal.resolved_copy
        numeric_labels = [
            label
            for label in internal.column_labels
            if isinstance(internal.spark_type_for(label), (NumericType, BooleanType))
        ]
        numeric_scols: List[Column] = [
            internal.spark_column_for(label).cast("double") for label in numeric_labels
        ]
        numeric_col_names: List[str] = [name_like_string(label) for label in numeric_labels]
        num_scols = len(numeric_scols)

        sdf = internal.spark_frame
        index_1_col_name = verify_temp_column_name(sdf, "__corr_index_1_temp_column__")
        index_2_col_name = verify_temp_column_name(sdf, "__corr_index_2_temp_column__")

        # simple dataset
        # +---+---+----+
        # |  A|  B|   C|
        # +---+---+----+
        # |  1|  2| 3.0|
        # |  4|  1|null|
        # +---+---+----+

        pair_scols: List[Column] = []
        for i in range(0, num_scols):
            for j in range(i, num_scols):
                pair_scols.append(
                    F.struct(
                        F.lit(i).alias(index_1_col_name),
                        F.lit(j).alias(index_2_col_name),
                        numeric_scols[i].alias(CORRELATION_VALUE_1_COLUMN),
                        numeric_scols[j].alias(CORRELATION_VALUE_2_COLUMN),
                    )
                )

        # +-------------------+-------------------+-------------------+-------------------+
        # |__tmp_index_1_col__|__tmp_index_2_col__|__tmp_value_1_col__|__tmp_value_2_col__|
        # +-------------------+-------------------+-------------------+-------------------+
        # |                  0|                  0|                1.0|                1.0|
        # |                  0|                  1|                1.0|                2.0|
        # |                  0|                  2|                1.0|                3.0|
        # |                  1|                  1|                2.0|                2.0|
        # |                  1|                  2|                2.0|                3.0|
        # |                  2|                  2|                3.0|                3.0|
        # |                  0|                  0|                4.0|                4.0|
        # |                  0|                  1|                4.0|                1.0|
        # |                  0|                  2|               null|               null|
        # |                  1|                  1|                1.0|                1.0|
        # |                  1|                  2|               null|               null|
        # |                  2|                  2|               null|               null|
        # +-------------------+-------------------+-------------------+-------------------+
        sdf = sdf.select(F.inline(F.array(*pair_scols)))

        sdf = compute(sdf=sdf, groupKeys=[index_1_col_name, index_2_col_name], method=method)
        if method == "kendall":
            sdf = sdf.withColumn(
                CORRELATION_CORR_OUTPUT_COLUMN,
                F.when(F.col(index_1_col_name) == F.col(index_2_col_name), F.lit(1.0)).otherwise(
                    F.col(CORRELATION_CORR_OUTPUT_COLUMN)
                ),
            )

        sdf = sdf.withColumn(
            CORRELATION_CORR_OUTPUT_COLUMN,
            F.when(F.col(CORRELATION_COUNT_OUTPUT_COLUMN) < min_periods, F.lit(None)).otherwise(
                F.col(CORRELATION_CORR_OUTPUT_COLUMN)
            ),
        )

        # +-------------------+-------------------+----------------+
        # |__tmp_index_1_col__|__tmp_index_2_col__|__tmp_corr_col__|
        # +-------------------+-------------------+----------------+
        # |                  2|                  2|            null|
        # |                  1|                  2|            null|
        # |                  2|                  1|            null|
        # |                  1|                  1|             1.0|
        # |                  0|                  0|             1.0|
        # |                  0|                  1|            -1.0|
        # |                  1|                  0|            -1.0|
        # |                  0|                  2|            null|
        # |                  2|                  0|            null|
        # +-------------------+-------------------+----------------+

        auxiliary_col_name = verify_temp_column_name(sdf, "__corr_auxiliary_temp_column__")
        sdf = sdf.withColumn(
            auxiliary_col_name,
            F.explode(
                F.when(
                    F.col(index_1_col_name) == F.col(index_2_col_name),
                    F.lit([0]),
                ).otherwise(F.lit([0, 1]))
            ),
        ).select(
            F.when(F.col(auxiliary_col_name) == 0, F.col(index_1_col_name))
            .otherwise(F.col(index_2_col_name))
            .alias(index_1_col_name),
            F.when(F.col(auxiliary_col_name) == 0, F.col(index_2_col_name))
            .otherwise(F.col(index_1_col_name))
            .alias(index_2_col_name),
            F.col(CORRELATION_CORR_OUTPUT_COLUMN),
        )

        # +-------------------+--------------------+
        # |__tmp_index_1_col__|   __tmp_array_col__|
        # +-------------------+--------------------+
        # |                  0|[{0, 1.0}, {1, -1...|
        # |                  1|[{0, -1.0}, {1, 1...|
        # |                  2|[{0, null}, {1, n...|
        # +-------------------+--------------------+
        array_col_name = verify_temp_column_name(sdf, "__corr_array_temp_column__")
        sdf = (
            sdf.groupby(index_1_col_name)
            .agg(
                F.array_sort(
                    F.collect_list(
                        F.struct(F.col(index_2_col_name), F.col(CORRELATION_CORR_OUTPUT_COLUMN))
                    )
                ).alias(array_col_name)
            )
            .orderBy(index_1_col_name)
        )

        for i in range(0, num_scols):
            sdf = sdf.withColumn(auxiliary_col_name, F.get(F.col(array_col_name), i)).withColumn(
                numeric_col_names[i],
                F.col(f"{auxiliary_col_name}.{CORRELATION_CORR_OUTPUT_COLUMN}"),
            )

        index_col_names: List[str] = []
        if internal.column_labels_level > 1:
            for level in range(0, internal.column_labels_level):
                index_col_name = SPARK_INDEX_NAME_FORMAT(level)
                indices = [label[level] for label in numeric_labels]
                sdf = sdf.withColumn(index_col_name, F.get(F.lit(indices), F.col(index_1_col_name)))
                index_col_names.append(index_col_name)
        else:
            sdf = sdf.withColumn(
                SPARK_DEFAULT_INDEX_NAME,
                F.get(F.lit(numeric_col_names), F.col(index_1_col_name)),
            )
            index_col_names = [SPARK_DEFAULT_INDEX_NAME]

        sdf = sdf.select(*index_col_names, *numeric_col_names)

        return DataFrame(
            InternalFrame(
                spark_frame=sdf,
                index_spark_columns=[
                    scol_for(sdf, index_col_name) for index_col_name in index_col_names
                ],
                column_labels=numeric_labels,
                column_label_names=internal.column_label_names,
            )
        )

    def corrwith(
        self, other: DataFrameOrSeries, axis: Axis = 0, drop: bool = False, method: str = "pearson"
    ) -> "Series":
        """
        Compute pairwise correlation.

        Pairwise correlation is computed between rows or columns of
        DataFrame with rows or columns of Series or DataFrame. DataFrames
        are first aligned along both axes before computing the
        correlations.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        other : DataFrame, Series
            Object with which to compute correlations.
        axis : int, default 0 or 'index'
            Can only be set to 0 now.
        drop : bool, default False
            Drop missing indices from result.
        method : {'pearson', 'spearman', 'kendall'}
            * pearson : standard correlation coefficient
            * spearman : Spearman rank correlation
            * kendall : Kendall Tau correlation coefficient

        Returns
        -------
        Series
            Pairwise correlations.

        See Also
        --------
        DataFrame.corr : Compute pairwise correlation of columns.

        Examples
        --------
        >>> df1 = ps.DataFrame({
        ...         "A":[1, 5, 7, 8],
        ...         "X":[5, 8, 4, 3],
        ...         "C":[10, 4, 9, 3]})
        >>> df1.corrwith(df1[["X", "C"]]).sort_index()
        A    NaN
        C    1.0
        X    1.0
        dtype: float64

        >>> df2 = ps.DataFrame({
        ...         "A":[5, 3, 6, 4],
        ...         "B":[11, 2, 4, 3],
        ...         "C":[4, 3, 8, 5]})

        >>> with ps.option_context("compute.ops_on_diff_frames", True):
        ...     df1.corrwith(df2).sort_index()
        A   -0.041703
        B         NaN
        C    0.395437
        X         NaN
        dtype: float64

        >>> with ps.option_context("compute.ops_on_diff_frames", True):
        ...     df1.corrwith(df2, method="kendall").sort_index()
        A    0.0
        B    NaN
        C    0.0
        X    NaN
        dtype: float64

        >>> with ps.option_context("compute.ops_on_diff_frames", True):
        ...     df1.corrwith(df2.B, method="spearman").sort_index()
        A   -0.4
        C    0.8
        X   -0.2
        dtype: float64

        >>> with ps.option_context("compute.ops_on_diff_frames", True):
        ...     df2.corrwith(df1.X).sort_index()
        A   -0.597614
        B   -0.151186
        C   -0.642857
        dtype: float64
        """
        from pyspark.pandas.series import Series, first_series

        axis = validate_axis(axis)
        if axis != 0:
            raise NotImplementedError("corrwith currently only works for axis=0")
        if method not in ["pearson", "spearman", "kendall"]:
            raise ValueError(f"Invalid method {method}")
        if not isinstance(other, (DataFrame, Series)):
            raise TypeError("unsupported type: {}".format(type(other).__name__))

        right_is_series = isinstance(other, Series)

        if same_anchor(self, other):
            combined = self
            this = self
            that = other
        else:
            combined = combine_frames(self, other, how="inner")
            this = combined["this"]
            that = combined["that"]

        sdf = combined._internal.spark_frame
        index_col_name = verify_temp_column_name(sdf, "__corrwith_index_temp_column__")

        this_numeric_column_labels: List[Label] = []
        for column_label in this._internal.column_labels:
            if isinstance(this._internal.spark_type_for(column_label), (NumericType, BooleanType)):
                this_numeric_column_labels.append(column_label)

        that_numeric_column_labels: List[Label] = []
        for column_label in that._internal.column_labels:
            if isinstance(that._internal.spark_type_for(column_label), (NumericType, BooleanType)):
                that_numeric_column_labels.append(column_label)

        intersect_numeric_column_labels: List[Label] = []
        diff_numeric_column_labels: List[Label] = []
        pair_scols: List[Column] = []
        if right_is_series:
            intersect_numeric_column_labels = this_numeric_column_labels
            that_scol = that._internal.spark_column_for(that_numeric_column_labels[0]).cast(
                "double"
            )
            for numeric_column_label in intersect_numeric_column_labels:
                this_scol = this._internal.spark_column_for(numeric_column_label).cast("double")
                pair_scols.append(
                    F.struct(
                        F.lit(name_like_string(numeric_column_label)).alias(index_col_name),
                        this_scol.alias(CORRELATION_VALUE_1_COLUMN),
                        that_scol.alias(CORRELATION_VALUE_2_COLUMN),
                    )
                )
        else:
            for numeric_column_label in this_numeric_column_labels:
                if numeric_column_label in that_numeric_column_labels:
                    intersect_numeric_column_labels.append(numeric_column_label)
                else:
                    diff_numeric_column_labels.append(numeric_column_label)
            for numeric_column_label in that_numeric_column_labels:
                if numeric_column_label not in this_numeric_column_labels:
                    diff_numeric_column_labels.append(numeric_column_label)
            for numeric_column_label in intersect_numeric_column_labels:
                this_scol = this._internal.spark_column_for(numeric_column_label).cast("double")
                that_scol = that._internal.spark_column_for(numeric_column_label).cast("double")
                pair_scols.append(
                    F.struct(
                        F.lit(name_like_string(numeric_column_label)).alias(index_col_name),
                        this_scol.alias(CORRELATION_VALUE_1_COLUMN),
                        that_scol.alias(CORRELATION_VALUE_2_COLUMN),
                    )
                )

        if len(pair_scols) > 0:
            sdf = sdf.select(F.inline(F.array(*pair_scols)))

            sdf = compute(sdf=sdf, groupKeys=[index_col_name], method=method).select(
                index_col_name, CORRELATION_CORR_OUTPUT_COLUMN
            )

        else:
            sdf = self._internal.spark_frame.select(
                F.lit(None).cast("string").alias(index_col_name),
                F.lit(None).cast("double").alias(CORRELATION_CORR_OUTPUT_COLUMN),
            ).limit(0)

        if not drop and len(diff_numeric_column_labels) > 0:
            sdf2 = (
                self._internal.spark_frame.select(
                    F.lit([name_like_string(label) for label in diff_numeric_column_labels]).alias(
                        index_col_name
                    )
                )
                .limit(1)
                .select(F.explode(index_col_name).alias(index_col_name))
            )
            sdf = sdf.unionByName(sdf2, allowMissingColumns=True)

        sdf = sdf.withColumn(
            NATURAL_ORDER_COLUMN_NAME,
            F.monotonically_increasing_id(),
        )

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, index_col_name)],
            column_labels=[(CORRELATION_CORR_OUTPUT_COLUMN,)],
            column_label_names=self._internal.column_label_names,
        )
        sser = first_series(DataFrame(internal))
        sser.name = None
        return sser

    def items(self) -> Iterator[Tuple[Name, "Series"]]:
        """
        Iterator over (column name, Series) pairs.

        Iterates over the DataFrame columns, returning a tuple with
        the column name and the content as a Series.

        Returns
        -------
        label : object
            The column names for the DataFrame being iterated over.
        content : Series
            The column entries belonging to each label, as a Series.

        Examples
        --------
        >>> df = ps.DataFrame({'species': ['bear', 'bear', 'marsupial'],
        ...                    'population': [1864, 22000, 80000]},
        ...                   index=['panda', 'polar', 'koala'],
        ...                   columns=['species', 'population'])
        >>> df
                 species  population
        panda       bear        1864
        polar       bear       22000
        koala  marsupial       80000

        >>> for label, content in df.iteritems():
        ...    print('label:', label)
        ...    print('content:', content.to_string())
        ...
        label: species
        content: panda         bear
        polar         bear
        koala    marsupial
        label: population
        content: panda     1864
        polar    22000
        koala    80000
        """
        return (
            (label if len(label) > 1 else label[0], self._psser_for(label))
            for label in self._internal.column_labels
        )

    def iterrows(self) -> Iterator[Tuple[Name, pd.Series]]:
        """
        Iterate over DataFrame rows as (index, Series) pairs.

        Yields
        ------
        index : label or tuple of label
            The index of the row. A tuple for a `MultiIndex`.
        data : pandas.Series
            The data of the row as a Series.

        it : generator
            A generator that iterates over the rows of the frame.

        Notes
        -----

        1. Because ``iterrows`` returns a Series for each row,
           it does **not** preserve dtypes across the rows (dtypes are
           preserved across columns for DataFrames). For example,

           >>> df = ps.DataFrame([[1, 1.5]], columns=['int', 'float'])
           >>> row = next(df.iterrows())[1]
           >>> row
           int      1.0
           float    1.5
           Name: 0, dtype: float64
           >>> print(row['int'].dtype)
           float64
           >>> print(df['int'].dtype)
           int64

           To preserve dtypes while iterating over the rows, it is better
           to use :meth:`itertuples` which returns namedtuples of the values
           and which is generally faster than ``iterrows``.

        2. You should **never modify** something you are iterating over.
           This is not guaranteed to work in all cases. Depending on the
           data types, the iterator returns a copy and not a view, and writing
           to it will have no effect.
        """

        columns = self.columns
        internal_index_columns = self._internal.index_spark_column_names
        internal_data_columns = self._internal.data_spark_column_names

        def extract_kv_from_spark_row(row: Row) -> Tuple[Name, Any]:
            k = (
                row[internal_index_columns[0]]
                if len(internal_index_columns) == 1
                else tuple(row[c] for c in internal_index_columns)
            )
            v = [row[c] for c in internal_data_columns]
            return k, v

        for k, v in map(
            extract_kv_from_spark_row, self._internal.resolved_copy.spark_frame.toLocalIterator()
        ):
            s = pd.Series(v, index=columns, name=k)
            yield k, s

    def itertuples(
        self, index: bool = True, name: Optional[str] = "PandasOnSpark"
    ) -> Iterator[Tuple]:
        """
        Iterate over DataFrame rows as namedtuples.

        Parameters
        ----------
        index : bool, default True
            If True, return the index as the first element of the tuple.
        name : str or None, default "PandasOnSpark"
            The name of the returned namedtuples or None to return regular
            tuples.

        Returns
        -------
        iterator
            An object to iterate over namedtuples for each row in the
            DataFrame with the first field possibly being the index and
            following fields being the column values.

        See Also
        --------
        DataFrame.iterrows : Iterate over DataFrame rows as (index, Series)
            pairs.
        DataFrame.items : Iterate over (column name, Series) pairs.

        Notes
        -----
        The column names will be renamed to positional names if they are
        invalid Python identifiers, repeated, or start with an underscore.
        On python versions < 3.7 regular tuples are returned for DataFrames
        with many columns (>254).

        Examples
        --------
        >>> df = ps.DataFrame({'num_legs': [4, 2], 'num_wings': [0, 2]},
        ...                   index=['dog', 'hawk'])
        >>> df
              num_legs  num_wings
        dog          4          0
        hawk         2          2

        >>> for row in df.itertuples():
        ...     print(row)
        ...
        PandasOnSpark(Index='dog', num_legs=4, num_wings=0)
        PandasOnSpark(Index='hawk', num_legs=2, num_wings=2)

        By setting the `index` parameter to False we can remove the index
        as the first element of the tuple:

        >>> for row in df.itertuples(index=False):
        ...     print(row)
        ...
        PandasOnSpark(num_legs=4, num_wings=0)
        PandasOnSpark(num_legs=2, num_wings=2)

        With the `name` parameter set we set a custom name for the yielded
        namedtuples:

        >>> for row in df.itertuples(name='Animal'):
        ...     print(row)
        ...
        Animal(Index='dog', num_legs=4, num_wings=0)
        Animal(Index='hawk', num_legs=2, num_wings=2)
        """
        fields = list(self.columns)
        if index:
            fields.insert(0, "Index")

        index_spark_column_names = self._internal.index_spark_column_names
        data_spark_column_names = self._internal.data_spark_column_names

        def extract_kv_from_spark_row(row: Row) -> Tuple[Name, Any]:
            k = (
                row[index_spark_column_names[0]]
                if len(index_spark_column_names) == 1
                else tuple(row[c] for c in index_spark_column_names)
            )
            v = [row[c] for c in data_spark_column_names]
            return k, v

        can_return_named_tuples = sys.version_info >= (3, 7) or len(self.columns) + index < 255

        if name is not None and can_return_named_tuples:
            itertuple = namedtuple(name, fields, rename=True)  # type: ignore[misc]
            for k, v in map(
                extract_kv_from_spark_row,
                self._internal.resolved_copy.spark_frame.toLocalIterator(),
            ):
                yield itertuple._make(([k] if index else []) + list(v))
        else:
            for k, v in map(
                extract_kv_from_spark_row,
                self._internal.resolved_copy.spark_frame.toLocalIterator(),
            ):
                yield tuple(([k] if index else []) + list(v))

    def iteritems(self) -> Iterator[Tuple[Name, "Series"]]:
        """
        This is an alias of ``items``.

        .. deprecated:: 3.4.0
            iteritems is deprecated and will be removed in a future version.
            Use .items instead.
        """
        warnings.warn("Deprecated in 3.4.0, Use DataFrame.items instead.", FutureWarning)
        return self.items()

    def to_clipboard(self, excel: bool = True, sep: Optional[str] = None, **kwargs: Any) -> None:
        """
        Copy object to the system clipboard.

        Write a text representation of object to the system clipboard.
        This can be pasted into Excel, for example.

        .. note:: This method should only be used if the resulting DataFrame is expected
            to be small, as all the data is loaded into the driver's memory.

        Parameters
        ----------
        excel : bool, default True
            - True, use the provided separator, writing in a csv format for
              allowing easy pasting into excel.
            - False, write a string representation of the object to the
              clipboard.

        sep : str, default ``'\\t'``
            Field delimiter.
        **kwargs
            These parameters will be passed to DataFrame.to_csv.

        Notes
        -----
        Requirements for your platform.

          - Linux : `xclip`, or `xsel` (with `gtk` or `PyQt4` modules)
          - Windows : none
          - OS X : none

        See Also
        --------
        read_clipboard : Read text from clipboard.

        Examples
        --------
        Copy the contents of a DataFrame to the clipboard.

        >>> df = ps.DataFrame([[1, 2, 3], [4, 5, 6]], columns=['A', 'B', 'C'])  # doctest: +SKIP
        >>> df.to_clipboard(sep=',')  # doctest: +SKIP
        ... # Wrote the following to the system clipboard:
        ... # ,A,B,C
        ... # 0,1,2,3
        ... # 1,4,5,6

        We can omit the index by passing the keyword `index` and setting
        it to false.

        >>> df.to_clipboard(sep=',', index=False)  # doctest: +SKIP
        ... # Wrote the following to the system clipboard:
        ... # A,B,C
        ... # 1,2,3
        ... # 4,5,6

        This function also works for Series:

        >>> df = ps.Series([1, 2, 3, 4, 5, 6, 7], name='x')  # doctest: +SKIP
        >>> df.to_clipboard(sep=',')  # doctest: +SKIP
        ... # Wrote the following to the system clipboard:
        ... # 0, 1
        ... # 1, 2
        ... # 2, 3
        ... # 3, 4
        ... # 4, 5
        ... # 5, 6
        ... # 6, 7
        """

        args = locals()
        psdf = self
        return validate_arguments_and_invoke_function(
            psdf._to_internal_pandas(), self.to_clipboard, pd.DataFrame.to_clipboard, args
        )

    def to_html(
        self,
        buf: Optional[IO[str]] = None,
        columns: Optional[Sequence[Name]] = None,
        col_space: Optional[Union[str, int, Dict[Name, Union[str, int]]]] = None,
        header: bool = True,
        index: bool = True,
        na_rep: str = "NaN",
        formatters: Optional[
            Union[List[Callable[[Any], str]], Dict[Name, Callable[[Any], str]]]
        ] = None,
        float_format: Optional[Callable[[float], str]] = None,
        sparsify: Optional[bool] = None,
        index_names: bool = True,
        justify: Optional[str] = None,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        show_dimensions: bool = False,
        decimal: str = ".",
        bold_rows: bool = True,
        classes: Optional[Union[str, list, tuple]] = None,
        escape: bool = True,
        notebook: bool = False,
        border: Optional[int] = None,
        table_id: Optional[str] = None,
        render_links: bool = False,
    ) -> Optional[str]:
        """
        Render a DataFrame as an HTML table.

        .. note:: This method should only be used if the resulting pandas object is expected
                  to be small, as all the data is loaded into the driver's memory. If the input
                  is large, set max_rows parameter.

        Parameters
        ----------
        buf : StringIO-like, optional
            Buffer to write to.
        columns : sequence, optional, default None
            The subset of columns to write. Writes all columns by default.
        col_space : int, optional
            The minimum width of each column.
        header : bool, optional
            Write out the column names. If a list of strings is given, it
            is assumed to be aliases for the column names
        index : bool, optional, default True
            Whether to print index (row) labels.
        na_rep : str, optional, default 'NaN'
            String representation of NAN to use.
        formatters : list or dict of one-param. functions, optional
            Formatter functions to apply to columns' elements by position or
            name.
            The result of each function must be a Unicode string.
            List must be of length equal to the number of columns.
        float_format : one-parameter function, optional, default None
            Formatter function to apply to columns' elements if they are
            floats. The result of this function must be a Unicode string.
        sparsify : bool, optional, default True
            Set to False for a DataFrame with a hierarchical index to print
            every multiindex key at each row.
        index_names : bool, optional, default True
            Prints the names of the indexes.
        justify : str, default None
            How to justify the column labels. If None uses the option from
            the print configuration (controlled by set_option), 'right' out
            of the box. Valid values are

            * left
            * right
            * center
            * justify
            * justify-all
            * start
            * end
            * inherit
            * match-parent
            * initial
            * unset.
        max_rows : int, optional
            Maximum number of rows to display in the console.
        max_cols : int, optional
            Maximum number of columns to display in the console.
        show_dimensions : bool, default False
            Display DataFrame dimensions (number of rows by number of columns).
        decimal : str, default '.'
            Character recognized as decimal separator, e.g. ',' in Europe.
        bold_rows : bool, default True
            Make the row labels bold in the output.
        classes : str or list or tuple, default None
            CSS class(es) to apply to the resulting html table.
        escape : bool, default True
            Convert the characters <, >, and & to HTML-safe sequences.
        notebook : {True, False}, default False
            Whether the generated HTML is for IPython Notebook.
        border : int
            A ``border=border`` attribute is included in the opening
            `<table>` tag. By default ``pd.options.html.border``.
        table_id : str, optional
            A css id is included in the opening `<table>` tag if specified.
        render_links : bool, default False
            Convert URLs to HTML links (only works with pandas 0.24+).

        Returns
        -------
        str (or Unicode, depending on data and options)
            String representation of the dataframe.

        See Also
        --------
        to_string : Convert DataFrame to a string.
        """
        # Make sure locals() call is at the top of the function so we don't capture local variables.
        args = locals()
        if max_rows is not None:
            psdf = self.head(max_rows)
        else:
            psdf = self

        return validate_arguments_and_invoke_function(
            psdf._to_internal_pandas(), self.to_html, pd.DataFrame.to_html, args
        )

    def to_string(
        self,
        buf: Optional[IO[str]] = None,
        columns: Optional[Sequence[Name]] = None,
        col_space: Optional[Union[str, int, Dict[Name, Union[str, int]]]] = None,
        header: bool = True,
        index: bool = True,
        na_rep: str = "NaN",
        formatters: Optional[
            Union[List[Callable[[Any], str]], Dict[Name, Callable[[Any], str]]]
        ] = None,
        float_format: Optional[Callable[[float], str]] = None,
        sparsify: Optional[bool] = None,
        index_names: bool = True,
        justify: Optional[str] = None,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        show_dimensions: bool = False,
        decimal: str = ".",
        line_width: Optional[int] = None,
    ) -> Optional[str]:
        """
        Render a DataFrame to a console-friendly tabular output.

        .. note:: This method should only be used if the resulting pandas object is expected
                  to be small, as all the data is loaded into the driver's memory. If the input
                  is large, set max_rows parameter.

        Parameters
        ----------
        buf : StringIO-like, optional
            Buffer to write to.
        columns : sequence, optional, default None
            The subset of columns to write. Writes all columns by default.
        col_space : int, optional
            The minimum width of each column.
        header : bool, optional
            Write out the column names. If a list of strings is given, it
            is assumed to be aliases for the column names
        index : bool, optional, default True
            Whether to print index (row) labels.
        na_rep : str, optional, default 'NaN'
            String representation of NAN to use.
        formatters : list or dict of one-param. functions, optional
            Formatter functions to apply to columns' elements by position or
            name.
            The result of each function must be a Unicode string.
            List must be of length equal to the number of columns.
        float_format : one-parameter function, optional, default None
            Formatter function to apply to columns' elements if they are
            floats. The result of this function must be a Unicode string.
        sparsify : bool, optional, default True
            Set to False for a DataFrame with a hierarchical index to print
            every multiindex key at each row.
        index_names : bool, optional, default True
            Prints the names of the indexes.
        justify : str, default None
            How to justify the column labels. If None uses the option from
            the print configuration (controlled by set_option), 'right' out
            of the box. Valid values are

            * left
            * right
            * center
            * justify
            * justify-all
            * start
            * end
            * inherit
            * match-parent
            * initial
            * unset.
        max_rows : int, optional
            Maximum number of rows to display in the console.
        max_cols : int, optional
            Maximum number of columns to display in the console.
        show_dimensions : bool, default False
            Display DataFrame dimensions (number of rows by number of columns).
        decimal : str, default '.'
            Character recognized as decimal separator, e.g. ',' in Europe.
        line_width : int, optional
            Width to wrap a line in characters.

        Returns
        -------
        str (or Unicode, depending on data and options)
            String representation of the dataframe.

        See Also
        --------
        to_html : Convert DataFrame to HTML.

        Examples
        --------
        >>> df = ps.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]}, columns=['col1', 'col2'])
        >>> print(df.to_string())
           col1  col2
        0     1     4
        1     2     5
        2     3     6

        >>> print(df.to_string(max_rows=2))
           col1  col2
        0     1     4
        1     2     5
        """
        # Make sure locals() call is at the top of the function so we don't capture local variables.
        args = locals()
        if max_rows is not None:
            psdf = self.head(max_rows)
        else:
            psdf = self

        return validate_arguments_and_invoke_function(
            psdf._to_internal_pandas(), self.to_string, pd.DataFrame.to_string, args
        )

    def to_dict(self, orient: str = "dict", into: Type = dict) -> Union[List, Mapping]:
        """
        Convert the DataFrame to a dictionary.

        The type of the key-value pairs can be customized with the parameters
        (see below).

        .. note:: This method should only be used if the resulting pandas DataFrame is expected
            to be small, as all the data is loaded into the driver's memory.

        Parameters
        ----------
        orient : str {'dict', 'list', 'series', 'split', 'records', 'index'}
            Determines the type of the values of the dictionary.

            - 'dict' (default) : dict like {column -> {index -> value}}
            - 'list' : dict like {column -> [values]}
            - 'series' : dict like {column -> Series(values)}
            - 'split' : dict like
              {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
            - 'records' : list like
              [{column -> value}, ... , {column -> value}]
            - 'index' : dict like {index -> {column -> value}}

            Abbreviations are allowed. `s` indicates `series` and `sp`
            indicates `split`.

        into : class, default dict
            The collections.abc.Mapping subclass used for all Mappings
            in the return value.  Can be the actual class or an empty
            instance of the mapping type you want.  If you want a
            collections.defaultdict, you must pass it initialized.

        Returns
        -------
        dict, list or collections.abc.Mapping
            Return a collections.abc.Mapping object representing the DataFrame.
            The resulting transformation depends on the `orient` parameter.

        Examples
        --------
        >>> df = ps.DataFrame({'col1': [1, 2],
        ...                    'col2': [0.5, 0.75]},
        ...                   index=['row1', 'row2'],
        ...                   columns=['col1', 'col2'])
        >>> df
              col1  col2
        row1     1  0.50
        row2     2  0.75

        >>> df_dict = df.to_dict()
        >>> sorted([(key, sorted(values.items())) for key, values in df_dict.items()])
        [('col1', [('row1', 1), ('row2', 2)]), ('col2', [('row1', 0.5), ('row2', 0.75)])]

        You can specify the return orientation.

        >>> df_dict = df.to_dict('series')
        >>> sorted(df_dict.items())
        [('col1', row1    1
        row2    2
        Name: col1, dtype: int64), ('col2', row1    0.50
        row2    0.75
        Name: col2, dtype: float64)]

        >>> df_dict = df.to_dict('split')
        >>> sorted(df_dict.items())  # doctest: +ELLIPSIS
        [('columns', ['col1', 'col2']), ('data', [[1..., 0.75]]), ('index', ['row1', 'row2'])]

        >>> df_dict = df.to_dict('records')
        >>> [sorted(values.items()) for values in df_dict]  # doctest: +ELLIPSIS
        [[('col1', 1...), ('col2', 0.5)], [('col1', 2...), ('col2', 0.75)]]

        >>> df_dict = df.to_dict('index')
        >>> sorted([(key, sorted(values.items())) for key, values in df_dict.items()])
        [('row1', [('col1', 1), ('col2', 0.5)]), ('row2', [('col1', 2), ('col2', 0.75)])]

        You can also specify the mapping type.

        >>> from collections import OrderedDict, defaultdict
        >>> df.to_dict(into=OrderedDict)
        OrderedDict([('col1', OrderedDict([('row1', 1), ('row2', 2)])), \
('col2', OrderedDict([('row1', 0.5), ('row2', 0.75)]))])

        If you want a `defaultdict`, you need to initialize it:

        >>> dd = defaultdict(list)
        >>> df.to_dict('records', into=dd)  # doctest: +ELLIPSIS
        [defaultdict(<class 'list'>, {'col..., 'col...}), \
defaultdict(<class 'list'>, {'col..., 'col...})]
        """
        # Make sure locals() call is at the top of the function so we don't capture local variables.
        args = locals()
        psdf = self
        return validate_arguments_and_invoke_function(
            psdf._to_internal_pandas(), self.to_dict, pd.DataFrame.to_dict, args
        )

    def to_latex(
        self,
        buf: Optional[IO[str]] = None,
        columns: Optional[List[Name]] = None,
        col_space: Optional[int] = None,
        header: bool = True,
        index: bool = True,
        na_rep: str = "NaN",
        formatters: Optional[
            Union[List[Callable[[Any], str]], Dict[Name, Callable[[Any], str]]]
        ] = None,
        float_format: Optional[Callable[[float], str]] = None,
        sparsify: Optional[bool] = None,
        index_names: bool = True,
        bold_rows: bool = False,
        column_format: Optional[str] = None,
        longtable: Optional[bool] = None,
        escape: Optional[bool] = None,
        encoding: Optional[str] = None,
        decimal: str = ".",
        multicolumn: Optional[bool] = None,
        multicolumn_format: Optional[str] = None,
        multirow: Optional[bool] = None,
    ) -> Optional[str]:
        r"""
        Render an object to a LaTeX tabular environment table.

        Render an object to a tabular environment table. You can splice this into a LaTeX
        document. Requires usepackage{booktabs}.

        .. note:: This method should only be used if the resulting pandas object is expected
                  to be small, as all the data is loaded into the driver's memory. If the input
                  is large, consider alternative formats.

        Parameters
        ----------
        buf : file descriptor or None
            Buffer to write to. If None, the output is returned as a string.
        columns : list of label, optional
            The subset of columns to write. Writes all columns by default.
        col_space : int, optional
            The minimum width of each column.

            .. deprecated:: 3.4.0

        header : bool or list of str, default True
            Write out the column names. If a list of strings is given, it is assumed to be aliases
            for the column names.
        index : bool, default True
            Write row names (index).
        na_rep : str, default ‘NaN’
            Missing data representation.
        formatters : list of functions or dict of {str: function}, optional
            Formatter functions to apply to columns’ elements by position or name. The result of
            each function must be a Unicode string. List must be of length equal to the number of
            columns.
        float_format : str, optional
            Format string for floating point numbers.
        sparsify : bool, optional
            Set to False for a DataFrame with a hierarchical index to print every multiindex key at
            each row. By default the value will be read from the config module.
        index_names : bool, default True
            Prints the names of the indexes.
        bold_rows : bool, default False
            Make the row labels bold in the output.
        column_format : str, optional
            The columns format as specified in LaTeX table format e.g. ‘rcl’ for 3 columns. By
            default, ‘l’ will be used for all columns except columns of numbers, which default
            to ‘r’.
        longtable : bool, optional
            By default the value will be read from the pandas config module. Use a longtable
            environment instead of tabular. Requires adding a usepackage{longtable} to your LaTeX
            preamble.
        escape : bool, optional
            By default the value will be read from the pandas config module. When set to False
            prevents from escaping latex special characters in column names.
        encoding : str, optional
            A string representing the encoding to use in the output file, defaults to ‘ascii’ on
            Python 2 and ‘utf-8’ on Python 3.
        decimal : str, default ‘.’
            Character recognized as decimal separator, e.g. ‘,’ in Europe.
        multicolumn : bool, default True
            Use multicolumn to enhance MultiIndex columns. The default will be read from the config
            module.
        multicolumn_format : str, default ‘l’
            The alignment for multicolumns, similar to column_format The default will be read from
            the config module.
        multirow : bool, default False
            Use multirow to enhance MultiIndex rows. Requires adding a usepackage{multirow} to your
            LaTeX preamble. Will print centered labels (instead of top-aligned) across the contained
            rows, separating groups via clines. The default will be read from the pandas config
            module.

        Returns
        -------
        str or None
            If buf is None, returns the resulting LateX format as a string. Otherwise returns None.

        See Also
        --------
        DataFrame.to_string : Render a DataFrame to a console-friendly
            tabular output.
        DataFrame.to_html : Render a DataFrame as an HTML table.


        Examples
        --------
        >>> df = ps.DataFrame({'name': ['Raphael', 'Donatello'],
        ...                    'mask': ['red', 'purple'],
        ...                    'weapon': ['sai', 'bo staff']},
        ...                   columns=['name', 'mask', 'weapon'])
        >>> print(df.to_latex(index=False)) # doctest: +NORMALIZE_WHITESPACE
        \begin{tabular}{lll}
        \toprule
              name &    mask &    weapon \\
        \midrule
           Raphael &     red &       sai \\
         Donatello &  purple &  bo staff \\
        \bottomrule
        \end{tabular}
        """

        args = locals()
        psdf = self
        return validate_arguments_and_invoke_function(
            psdf._to_internal_pandas(), self.to_latex, pd.DataFrame.to_latex, args
        )

    # TODO: enable doctests once we drop Spark 2.3.x (due to type coercion logic
    #  when creating arrays)
    def transpose(self) -> "DataFrame":
        """
        Transpose index and columns.

        Reflect the DataFrame over its main diagonal by writing rows as columns
        and vice-versa. The property :attr:`.T` is an accessor to the method
        :meth:`transpose`.

        .. note:: This method is based on an expensive operation due to the nature
            of big data. Internally it needs to generate each row for each value, and
            then group twice - it is a huge operation. To prevent misuse, this method
            has the 'compute.max_rows' default limit of input length and raises a ValueError.

                >>> from pyspark.pandas.config import option_context
                >>> with option_context('compute.max_rows', 1000):  # doctest: +NORMALIZE_WHITESPACE
                ...     ps.DataFrame({'a': range(1001)}).transpose()
                Traceback (most recent call last):
                  ...
                ValueError: Current DataFrame's length exceeds the given limit of 1000 rows.
                Please set 'compute.max_rows' by using 'pyspark.pandas.config.set_option'
                to retrieve more than 1000 rows. Note that, before changing the
                'compute.max_rows', this operation is considerably expensive.

        Returns
        -------
        DataFrame
            The transposed DataFrame.

        Notes
        -----
        Transposing a DataFrame with mixed dtypes will result in a homogeneous
        DataFrame with the coerced dtype. For instance, if int and float have
        to be placed in same column, it becomes float. If type coercion is not
        possible, it fails.

        Also, note that the values in index should be unique because they become
        unique column names.

        In addition, if Spark 2.3 is used, the types should always be exactly same.

        Examples
        --------
        **Square DataFrame with homogeneous dtype**

        >>> d1 = {'col1': [1, 2], 'col2': [3, 4]}
        >>> df1 = ps.DataFrame(data=d1, columns=['col1', 'col2'])
        >>> df1
           col1  col2
        0     1     3
        1     2     4

        >>> df1_transposed = df1.T.sort_index()  # doctest: +SKIP
        >>> df1_transposed  # doctest: +SKIP
              0  1
        col1  1  2
        col2  3  4

        When the dtype is homogeneous in the original DataFrame, we get a
        transposed DataFrame with the same dtype:

        >>> df1.dtypes
        col1    int64
        col2    int64
        dtype: object
        >>> df1_transposed.dtypes  # doctest: +SKIP
        0    int64
        1    int64
        dtype: object

        **Non-square DataFrame with mixed dtypes**

        >>> d2 = {'score': [9.5, 8],
        ...       'kids': [0, 0],
        ...       'age': [12, 22]}
        >>> df2 = ps.DataFrame(data=d2, columns=['score', 'kids', 'age'])
        >>> df2
           score  kids  age
        0    9.5     0   12
        1    8.0     0   22

        >>> df2_transposed = df2.T.sort_index()  # doctest: +SKIP
        >>> df2_transposed  # doctest: +SKIP
                  0     1
        age    12.0  22.0
        kids    0.0   0.0
        score   9.5   8.0

        When the DataFrame has mixed dtypes, we get a transposed DataFrame with
        the coerced dtype:

        >>> df2.dtypes
        score    float64
        kids       int64
        age        int64
        dtype: object

        >>> df2_transposed.dtypes  # doctest: +SKIP
        0    float64
        1    float64
        dtype: object
        """
        max_compute_count = get_option("compute.max_rows")
        if max_compute_count is not None:
            pdf = self.head(max_compute_count + 1)._to_internal_pandas()
            if len(pdf) > max_compute_count:
                raise ValueError(
                    "Current DataFrame's length exceeds the given limit of {0} rows. "
                    "Please set 'compute.max_rows' by using 'pyspark.pandas.config.set_option' "
                    "to retrieve more than {0} rows. Note that, before changing the "
                    "'compute.max_rows', this operation is considerably expensive.".format(
                        max_compute_count
                    )
                )
            return DataFrame(pdf.transpose())

        # Explode the data to be pairs.
        #
        # For instance, if the current input DataFrame is as below:
        #
        # +------+------+------+------+------+
        # |index1|index2|(a,x1)|(a,x2)|(b,x3)|
        # +------+------+------+------+------+
        # |    y1|    z1|     1|     0|     0|
        # |    y2|    z2|     0|    50|     0|
        # |    y3|    z3|     3|     2|     1|
        # +------+------+------+------+------+
        #
        # Output of `exploded_df` becomes as below:
        #
        # +-----------------+-----------------+-----------------+-----+
        # |            index|__index_level_0__|__index_level_1__|value|
        # +-----------------+-----------------+-----------------+-----+
        # |{"a":["y1","z1"]}|                a|               x1|    1|
        # |{"a":["y1","z1"]}|                a|               x2|    0|
        # |{"a":["y1","z1"]}|                b|               x3|    0|
        # |{"a":["y2","z2"]}|                a|               x1|    0|
        # |{"a":["y2","z2"]}|                a|               x2|   50|
        # |{"a":["y2","z2"]}|                b|               x3|    0|
        # |{"a":["y3","z3"]}|                a|               x1|    3|
        # |{"a":["y3","z3"]}|                a|               x2|    2|
        # |{"a":["y3","z3"]}|                b|               x3|    1|
        # +-----------------+-----------------+-----------------+-----+
        pairs = F.explode(
            F.array(
                *[
                    F.struct(
                        *[
                            F.lit(col).alias(SPARK_INDEX_NAME_FORMAT(i))
                            for i, col in enumerate(label)
                        ],
                        *[self._internal.spark_column_for(label).alias("value")],
                    )
                    for label in self._internal.column_labels
                ]
            )
        )

        exploded_df = self._internal.spark_frame.withColumn("pairs", pairs).select(
            [
                F.to_json(
                    F.struct(
                        F.array(*[scol for scol in self._internal.index_spark_columns]).alias("a")
                    )
                ).alias("index"),
                F.col("pairs.*"),
            ]
        )

        # After that, executes pivot with key and its index column.
        # Note that index column should contain unique values since column names
        # should be unique.
        internal_index_columns = [
            SPARK_INDEX_NAME_FORMAT(i) for i in range(self._internal.column_labels_level)
        ]
        pivoted_df = exploded_df.groupBy(internal_index_columns).pivot("index")

        transposed_df = pivoted_df.agg(F.first(F.col("value")))

        new_data_columns = list(
            filter(lambda x: x not in internal_index_columns, transposed_df.columns)
        )

        column_labels = [
            None if len(label) == 1 and label[0] is None else label
            for label in (tuple(json.loads(col)["a"]) for col in new_data_columns)
        ]

        internal = InternalFrame(
            spark_frame=transposed_df,
            index_spark_columns=[scol_for(transposed_df, col) for col in internal_index_columns],
            index_names=self._internal.column_label_names,
            column_labels=column_labels,
            data_spark_columns=[scol_for(transposed_df, col) for col in new_data_columns],
            column_label_names=self._internal.index_names,
        )

        return DataFrame(internal)

    T = property(transpose)

    def apply(
        self, func: Callable, axis: Axis = 0, args: Sequence[Any] = (), **kwds: Any
    ) -> Union["Series", "DataFrame", "Index"]:
        """
        Apply a function along an axis of the DataFrame.

        Objects passed to the function are Series objects whose index is
        either the DataFrame's index (``axis=0``) or the DataFrame's columns
        (``axis=1``).

        See also `Transform and apply a function
        <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/transform_apply.html>`_.

        .. note:: when `axis` is 0 or 'index', the `func` is unable to access
            to the whole input series. pandas-on-Spark internally splits the input series into
            multiple batches and calls `func` with each batch multiple times. Therefore, operations
            such as global aggregations are impossible. See the example below.

            >>> # This case does not return the length of whole series but of the batch internally
            ... # used.
            ... def length(s) -> int:
            ...     return len(s)
            ...
            >>> df = ps.DataFrame({'A': range(1000)})
            >>> df.apply(length, axis=0)  # doctest: +SKIP
            0     83
            1     83
            2     83
            ...
            10    83
            11    83
            dtype: int32

        .. note:: this API executes the function once to infer the type which is
            potentially expensive, for instance, when the dataset is created after
            aggregations or sorting.

            To avoid this, specify the return type as `Series` or scalar value in ``func``,
            for instance, as below:

            >>> def square(s) -> ps.Series[np.int32]:
            ...     return s ** 2

            pandas-on-Spark uses return type hints and does not try to infer the type.

            In case when axis is 1, it requires to specify `DataFrame` or scalar value
            with type hints as below:

            >>> def plus_one(x) -> ps.DataFrame[int, [float, float]]:
            ...     return x + 1

            If the return type is specified as `DataFrame`, the output column names become
            `c0, c1, c2 ... cn`. These names are positionally mapped to the returned
            DataFrame in ``func``.

            To specify the column names, you can assign them in a pandas style as below:

            >>> def plus_one(x) -> ps.DataFrame[("index", int), [("a", float), ("b", float)]]:
            ...     return x + 1

            >>> pdf = pd.DataFrame({'a': [1, 2, 3], 'b': [3, 4, 5]})
            >>> def plus_one(x) -> ps.DataFrame[
            ...         (pdf.index.name, pdf.index.dtype), zip(pdf.dtypes, pdf.columns)]:
            ...     return x + 1

        Parameters
        ----------
        func : function
            Function to apply to each column or row.
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Axis along which the function is applied:

            * 0 or 'index': apply function to each column.
            * 1 or 'columns': apply function to each row.
        args : tuple
            Positional arguments to pass to `func` in addition to the
            array/series.
        **kwds
            Additional keyword arguments to pass as keywords arguments to
            `func`.

        Returns
        -------
        Series or DataFrame
            Result of applying ``func`` along the given axis of the
            DataFrame.

        See Also
        --------
        DataFrame.applymap : For elementwise operations.
        DataFrame.aggregate : Only perform aggregating type operations.
        DataFrame.transform : Only perform transforming type operations.
        Series.apply : The equivalent function for Series.

        Examples
        --------
        >>> df = ps.DataFrame([[4, 9]] * 3, columns=['A', 'B'])
        >>> df
           A  B
        0  4  9
        1  4  9
        2  4  9

        Using a numpy universal function (in this case the same as
        ``np.sqrt(df)``):

        >>> def sqrt(x) -> ps.Series[float]:
        ...     return np.sqrt(x)
        ...
        >>> df.apply(sqrt, axis=0)
             A    B
        0  2.0  3.0
        1  2.0  3.0
        2  2.0  3.0

        You can omit type hints and let pandas-on-Spark infer its type.

        >>> df.apply(np.sqrt, axis=0)
             A    B
        0  2.0  3.0
        1  2.0  3.0
        2  2.0  3.0

        When `axis` is 1 or 'columns', it applies the function for each row.

        >>> def summation(x) -> np.int64:
        ...     return np.sum(x)
        ...
        >>> df.apply(summation, axis=1)
        0    13
        1    13
        2    13
        dtype: int64

        You can omit type hints and let pandas-on-Spark infer its type.

        >>> df.apply(np.sum, axis=1)
        0    13
        1    13
        2    13
        dtype: int64

        >>> df.apply(max, axis=1)
        0    9
        1    9
        2    9
        dtype: int64

        Returning a list-like will result in a Series

        >>> df.apply(lambda x: [1, 2], axis=1)
        0    [1, 2]
        1    [1, 2]
        2    [1, 2]
        dtype: object

        To specify the types when `axis` is '1', it should use DataFrame[...]
        annotation. In this case, the column names are automatically generated.

        >>> def identify(x) -> ps.DataFrame[('index', int), [('A', np.int64), ('B', np.int64)]]:
        ...     return x
        ...
        >>> df.apply(identify, axis=1)  # doctest: +NORMALIZE_WHITESPACE
               A  B
        index
        0      4  9
        1      4  9
        2      4  9

        You can also specify extra arguments.

        >>> def plus_two(a, b, c) -> ps.DataFrame[np.int64, [np.int64, np.int64]]:
        ...     return a + b + c
        ...
        >>> df.apply(plus_two, axis=1, args=(1,), c=3)
           c0  c1
        0   8  13
        1   8  13
        2   8  13
        """
        from pyspark.pandas.groupby import GroupBy
        from pyspark.pandas.series import first_series

        if not isinstance(func, types.FunctionType):
            assert callable(func), "the first argument should be a callable function."
            f = func
            # Note that the return type hints specified here affects actual return
            # type in Spark (e.g., infer_return_type). And MyPy does not allow
            # redefinition of a function.
            func = lambda *args, **kwargs: f(*args, **kwargs)  # noqa: E731

        axis = validate_axis(axis)
        should_return_series = False
        spec = inspect.getfullargspec(func)
        return_sig = spec.annotations.get("return", None)
        should_infer_schema = return_sig is None
        should_retain_index = should_infer_schema

        def apply_func(pdf: pd.DataFrame) -> pd.DataFrame:
            pdf_or_pser = pdf.apply(func, axis=axis, args=args, **kwds)  # type: ignore[arg-type]
            if isinstance(pdf_or_pser, pd.Series):
                return pdf_or_pser.to_frame()
            else:
                return pdf_or_pser

        self_applied: DataFrame = DataFrame(self._internal.resolved_copy)

        column_labels: Optional[List[Label]] = None
        if should_infer_schema:
            # Here we execute with the first 1000 to get the return type.
            # If the records were less than 1000, it uses pandas API directly for a shortcut.
            log_advice(
                "If the type hints is not specified for `apply`, "
                "it is expensive to infer the data type internally."
            )
            limit = get_option("compute.shortcut_limit")
            pdf = self_applied.head(limit + 1)._to_internal_pandas()
            applied = pdf.apply(func, axis=axis, args=args, **kwds)  # type: ignore[arg-type]
            psser_or_psdf = ps.from_pandas(applied)
            if len(pdf) <= limit:
                return psser_or_psdf

            psdf = psser_or_psdf
            if isinstance(psser_or_psdf, ps.Series):
                should_return_series = True
                psdf = psser_or_psdf._psdf

            index_fields = [field.normalize_spark_type() for field in psdf._internal.index_fields]
            data_fields = [field.normalize_spark_type() for field in psdf._internal.data_fields]

            return_schema = StructType([field.struct_field for field in index_fields + data_fields])

            output_func = GroupBy._make_pandas_df_builder_func(
                self_applied, apply_func, return_schema, retain_index=should_retain_index
            )
            sdf = self_applied._internal.to_internal_spark_frame.mapInPandas(
                lambda iterator: map(output_func, iterator), schema=return_schema
            )

            # If schema is inferred, we can restore indexes too.
            internal = psdf._internal.with_new_sdf(
                spark_frame=sdf, index_fields=index_fields, data_fields=data_fields
            )
        else:
            return_type = infer_return_type(func)
            require_index_axis = isinstance(return_type, SeriesType)
            require_column_axis = isinstance(return_type, DataFrameType)
            index_fields = None

            if require_index_axis:
                if axis != 0:
                    raise TypeError(
                        "The given function should specify a scalar or a series as its type "
                        "hints when axis is 0 or 'index'; however, the return type "
                        "was %s" % return_sig
                    )
                dtype = cast(SeriesType, return_type).dtype
                spark_type = cast(SeriesType, return_type).spark_type
                data_fields = [
                    InternalField(
                        dtype=dtype, struct_field=StructField(name=name, dataType=spark_type)
                    )
                    for name in self_applied.columns
                ]
                return_schema = StructType([field.struct_field for field in data_fields])
            elif require_column_axis:
                if axis != 1:
                    raise TypeError(
                        "The given function should specify a scalar or a frame as its type "
                        "hints when axis is 1 or 'column'; however, the return type "
                        "was %s" % return_sig
                    )
                index_fields = cast(DataFrameType, return_type).index_fields
                should_retain_index = len(index_fields) > 0
                data_fields = cast(DataFrameType, return_type).data_fields
                return_schema = cast(DataFrameType, return_type).spark_type
            else:
                # any axis is fine.
                should_return_series = True
                spark_type = cast(ScalarType, return_type).spark_type
                dtype = cast(ScalarType, return_type).dtype
                data_fields = [
                    InternalField(
                        dtype=dtype,
                        struct_field=StructField(
                            name=SPARK_DEFAULT_SERIES_NAME, dataType=spark_type
                        ),
                    )
                ]
                return_schema = StructType([field.struct_field for field in data_fields])
                column_labels = [None]

            output_func = GroupBy._make_pandas_df_builder_func(
                self_applied, apply_func, return_schema, retain_index=should_retain_index
            )
            sdf = self_applied._internal.to_internal_spark_frame.mapInPandas(
                lambda iterator: map(output_func, iterator), schema=return_schema
            )

            index_spark_columns = None
            index_names: Optional[List[Optional[Tuple[Any, ...]]]] = None

            if should_retain_index:
                index_spark_columns = [
                    scol_for(sdf, index_field.struct_field.name) for index_field in index_fields
                ]

                if not any(
                    [
                        SPARK_INDEX_NAME_PATTERN.match(index_field.struct_field.name)
                        for index_field in index_fields
                    ]
                ):
                    index_names = [(index_field.struct_field.name,) for index_field in index_fields]
            internal = InternalFrame(
                spark_frame=sdf,
                index_names=index_names,
                index_spark_columns=index_spark_columns,
                index_fields=index_fields,
                data_fields=data_fields,
                column_labels=column_labels,
            )

        result: DataFrame = DataFrame(internal)
        if should_return_series:
            return first_series(result)
        else:
            return result

    def transform(
        self, func: Callable[..., "Series"], axis: Axis = 0, *args: Any, **kwargs: Any
    ) -> "DataFrame":
        """
        Call ``func`` on self producing a Series with transformed values
        and that has the same length as its input.

        See also `Transform and apply a function
        <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/transform_apply.html>`_.

        .. note:: this API executes the function once to infer the type which is
             potentially expensive, for instance, when the dataset is created after
             aggregations or sorting.

             To avoid this, specify return type in ``func``, for instance, as below:

             >>> def square(x) -> ps.Series[np.int32]:
             ...     return x ** 2

             pandas-on-Spark uses return type hints and does not try to infer the type.

        .. note:: the series within ``func`` is actually multiple pandas series as the
            segments of the whole pandas-on-Spark series; therefore, the length of each series
            is not guaranteed. As an example, an aggregation against each series
            does work as a global aggregation but an aggregation of each segment. See
            below:

            >>> def func(x) -> ps.Series[np.int32]:
            ...     return x + sum(x)

        Parameters
        ----------
        func : function
            Function to use for transforming the data. It must work when pandas Series
            is passed.
        axis : int, default 0 or 'index'
            Can only be set to 0 now.
        *args
            Positional arguments to pass to func.
        **kwargs
            Keyword arguments to pass to func.

        Returns
        -------
        DataFrame
            A DataFrame that must have the same length as self.

        Raises
        ------
        Exception : If the returned DataFrame has a different length than self.

        See Also
        --------
        DataFrame.aggregate : Only perform aggregating type operations.
        DataFrame.apply : Invoke function on DataFrame.
        Series.transform : The equivalent function for Series.

        Examples
        --------
        >>> df = ps.DataFrame({'A': range(3), 'B': range(1, 4)}, columns=['A', 'B'])
        >>> df
           A  B
        0  0  1
        1  1  2
        2  2  3

        >>> def square(x) -> ps.Series[np.int32]:
        ...     return x ** 2
        >>> df.transform(square)
           A  B
        0  0  1
        1  1  4
        2  4  9

        You can omit type hints and let pandas-on-Spark infer its type.

        >>> df.transform(lambda x: x ** 2)
           A  B
        0  0  1
        1  1  4
        2  4  9

        For multi-index columns:

        >>> df.columns = [('X', 'A'), ('X', 'B')]
        >>> df.transform(square)  # doctest: +NORMALIZE_WHITESPACE
           X
           A  B
        0  0  1
        1  1  4
        2  4  9

        >>> (df * -1).transform(abs)  # doctest: +NORMALIZE_WHITESPACE
           X
           A  B
        0  0  1
        1  1  2
        2  2  3

        You can also specify extra arguments.

        >>> def calculation(x, y, z) -> ps.Series[int]:
        ...     return x ** y + z
        >>> df.transform(calculation, y=10, z=20)  # doctest: +NORMALIZE_WHITESPACE
              X
              A      B
        0    20     21
        1    21   1044
        2  1044  59069
        """
        if not isinstance(func, types.FunctionType):
            assert callable(func), "the first argument should be a callable function."
            f = func
            # Note that the return type hints specified here affects actual return
            # type in Spark (e.g., infer_return_type). And, MyPy does not allow
            # redefinition of a function.
            func = lambda *args, **kwargs: f(*args, **kwargs)  # noqa: E731

        axis = validate_axis(axis)
        if axis != 0:
            raise NotImplementedError('axis should be either 0 or "index" currently.')

        spec = inspect.getfullargspec(func)
        return_sig = spec.annotations.get("return", None)
        should_infer_schema = return_sig is None

        if should_infer_schema:
            # Here we execute with the first 1000 to get the return type.
            # If the records were less than 1000, it uses pandas API directly for a shortcut.
            log_advice(
                "If the type hints is not specified for `transform`, "
                "it is expensive to infer the data type internally."
            )
            limit = get_option("compute.shortcut_limit")
            pdf = self.head(limit + 1)._to_internal_pandas()
            transformed = pdf.transform(func, axis, *args, **kwargs)  # type: ignore[arg-type]
            psdf: DataFrame = DataFrame(transformed)
            if len(pdf) <= limit:
                return psdf

            applied = []
            data_fields = []
            for input_label, output_label in zip(
                self._internal.column_labels, psdf._internal.column_labels
            ):
                psser = self._psser_for(input_label)

                field = psdf._internal.field_for(output_label).normalize_spark_type()
                data_fields.append(field)

                return_schema = field.spark_type
                applied.append(
                    psser.pandas_on_spark._transform_batch(
                        func=lambda c: func(c, *args, **kwargs),
                        return_type=SeriesType(field.dtype, return_schema),
                    )
                )

            internal = self._internal.with_new_columns(applied, data_fields=data_fields)
            return DataFrame(internal)
        else:
            return self._apply_series_op(
                lambda psser: psser.pandas_on_spark.transform_batch(func, *args, **kwargs)
            )

    def pop(self, item: Name) -> "DataFrame":
        """
        Return item and drop from frame. Raise KeyError if not found.

        Parameters
        ----------
        item : str
            Label of column to be popped.

        Returns
        -------
        Series

        Examples
        --------
        >>> df = ps.DataFrame([('falcon', 'bird', 389.0),
        ...                    ('parrot', 'bird', 24.0),
        ...                    ('lion', 'mammal', 80.5),
        ...                    ('monkey','mammal', np.nan)],
        ...                   columns=('name', 'class', 'max_speed'))

        >>> df
             name   class  max_speed
        0  falcon    bird      389.0
        1  parrot    bird       24.0
        2    lion  mammal       80.5
        3  monkey  mammal        NaN

        >>> df.pop('class')
        0      bird
        1      bird
        2    mammal
        3    mammal
        Name: class, dtype: object

        >>> df
             name  max_speed
        0  falcon      389.0
        1  parrot       24.0
        2    lion       80.5
        3  monkey        NaN

        Also support for MultiIndex

        >>> df = ps.DataFrame([('falcon', 'bird', 389.0),
        ...                    ('parrot', 'bird', 24.0),
        ...                    ('lion', 'mammal', 80.5),
        ...                    ('monkey','mammal', np.nan)],
        ...                   columns=('name', 'class', 'max_speed'))
        >>> columns = [('a', 'name'), ('a', 'class'), ('b', 'max_speed')]
        >>> df.columns = pd.MultiIndex.from_tuples(columns)
        >>> df
                a                 b
             name   class max_speed
        0  falcon    bird     389.0
        1  parrot    bird      24.0
        2    lion  mammal      80.5
        3  monkey  mammal       NaN

        >>> df.pop('a')
             name   class
        0  falcon    bird
        1  parrot    bird
        2    lion  mammal
        3  monkey  mammal

        >>> df
                  b
          max_speed
        0     389.0
        1      24.0
        2      80.5
        3       NaN
        """
        result = self[item]
        self._update_internal_frame(self.drop(columns=item)._internal)
        return result

    # TODO: add axis parameter can work when '1' or 'columns'
    def xs(self, key: Name, axis: Axis = 0, level: Optional[int] = None) -> DataFrameOrSeries:
        """
        Return cross-section from the DataFrame.

        This method takes a `key` argument to select data at a particular
        level of a MultiIndex.

        Parameters
        ----------
        key : label or tuple of label
            Label contained in the index, or partially in a MultiIndex.
        axis : 0 or 'index', default 0
            Axis to retrieve cross-section on.
            currently only support 0 or 'index'
        level : object, defaults to first n levels (n=1 or len(key))
            In case of a key partially contained in a MultiIndex, indicate
            which levels are used. Levels can be referred by label or position.

        Returns
        -------
        DataFrame or Series
            Cross-section from the original DataFrame
            corresponding to the selected index levels.

        See Also
        --------
        DataFrame.loc : Access a group of rows and columns
            by label(s) or a boolean array.
        DataFrame.iloc : Purely integer-location based indexing
            for selection by position.

        Examples
        --------
        >>> d = {'num_legs': [4, 4, 2, 2],
        ...      'num_wings': [0, 0, 2, 2],
        ...      'class': ['mammal', 'mammal', 'mammal', 'bird'],
        ...      'animal': ['cat', 'dog', 'bat', 'penguin'],
        ...      'locomotion': ['walks', 'walks', 'flies', 'walks']}
        >>> df = ps.DataFrame(data=d)
        >>> df = df.set_index(['class', 'animal', 'locomotion'])
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                                   num_legs  num_wings
        class  animal  locomotion
        mammal cat     walks              4          0
               dog     walks              4          0
               bat     flies              2          2
        bird   penguin walks              2          2

        Get values at specified index

        >>> df.xs('mammal')  # doctest: +NORMALIZE_WHITESPACE
                           num_legs  num_wings
        animal locomotion
        cat    walks              4          0
        dog    walks              4          0
        bat    flies              2          2

        Get values at several indexes

        >>> df.xs(('mammal', 'dog'))  # doctest: +NORMALIZE_WHITESPACE
                    num_legs  num_wings
        locomotion
        walks              4          0

        >>> df.xs(('mammal', 'dog', 'walks'))  # doctest: +NORMALIZE_WHITESPACE
        num_legs     4
        num_wings    0
        Name: (mammal, dog, walks), dtype: int64

        Get values at specified index and level

        >>> df.xs('cat', level=1)  # doctest: +NORMALIZE_WHITESPACE
                           num_legs  num_wings
        class  locomotion
        mammal walks              4          0
        """
        from pyspark.pandas.series import first_series

        if not is_name_like_value(key):
            raise TypeError("'key' should be a scalar value or tuple that contains scalar values")

        if level is not None and is_name_like_tuple(key):
            raise KeyError(key)

        axis = validate_axis(axis)
        if axis != 0:
            raise NotImplementedError('axis should be either 0 or "index" currently.')

        if not is_name_like_tuple(key):
            key = (key,)
        if len(key) > self._internal.index_level:
            raise KeyError(
                "Key length ({}) exceeds index depth ({})".format(
                    len(key), self._internal.index_level
                )
            )
        if level is None:
            level = 0

        rows = [
            self._internal.index_spark_columns[lvl] == index for lvl, index in enumerate(key, level)
        ]
        internal = self._internal.with_filter(reduce(lambda x, y: x & y, rows))

        if len(key) == self._internal.index_level:
            psdf: DataFrame = DataFrame(internal)
            pdf = psdf.head(2)._to_internal_pandas()
            if len(pdf) == 0:
                raise KeyError(key)
            elif len(pdf) > 1:
                return psdf
            else:
                return first_series(DataFrame(pdf.transpose()))
        else:
            index_spark_columns = (
                internal.index_spark_columns[:level]
                + internal.index_spark_columns[level + len(key) :]
            )
            index_names = internal.index_names[:level] + internal.index_names[level + len(key) :]
            index_fields = internal.index_fields[:level] + internal.index_fields[level + len(key) :]

            internal = internal.copy(
                index_spark_columns=index_spark_columns,
                index_names=index_names,
                index_fields=index_fields,
            ).resolved_copy
            return DataFrame(internal)

    # TODO(SPARK-42620): Add `inclusive` parameter and replace `include_start` & `include_end`.
    # See https://github.com/pandas-dev/pandas/issues/43248
    def between_time(
        self,
        start_time: Union[datetime.time, str],
        end_time: Union[datetime.time, str],
        include_start: bool = True,
        include_end: bool = True,
        axis: Axis = 0,
    ) -> "DataFrame":
        """
        Select values between particular times of the day (example: 9:00-9:30 AM).

        By setting ``start_time`` to be later than ``end_time``,
        you can get the times that are *not* between the two times.

        Parameters
        ----------
        start_time : datetime.time or str
            Initial time as a time filter limit.
        end_time : datetime.time or str
            End time as a time filter limit.
        include_start : bool, default True
            Whether the start time needs to be included in the result.

            .. deprecated:: 3.4.0

        include_end : bool, default True
            Whether the end time needs to be included in the result.

            .. deprecated:: 3.4.0

        axis : {0 or 'index', 1 or 'columns'}, default 0
            Determine range time on index or columns value.

        Returns
        -------
        DataFrame
            Data from the original object filtered to the specified dates range.

        Raises
        ------
        TypeError
            If the index is not  a :class:`DatetimeIndex`

        See Also
        --------
        at_time : Select values at a particular time of the day.
        first : Select initial periods of time series based on a date offset.
        last : Select final periods of time series based on a date offset.
        DatetimeIndex.indexer_between_time : Get just the index locations for
            values between particular times of the day.

        Examples
        --------
        >>> idx = pd.date_range('2018-04-09', periods=4, freq='1D20min')
        >>> psdf = ps.DataFrame({'A': [1, 2, 3, 4]}, index=idx)
        >>> psdf
                             A
        2018-04-09 00:00:00  1
        2018-04-10 00:20:00  2
        2018-04-11 00:40:00  3
        2018-04-12 01:00:00  4

        >>> psdf.between_time('0:15', '0:45')
                             A
        2018-04-10 00:20:00  2
        2018-04-11 00:40:00  3

        You get the times that are *not* between two times by setting
        ``start_time`` later than ``end_time``:

        >>> psdf.between_time('0:45', '0:15')
                             A
        2018-04-09 00:00:00  1
        2018-04-12 01:00:00  4
        """
        axis = validate_axis(axis)

        if axis != 0:
            raise NotImplementedError("between_time currently only works for axis=0")

        if not isinstance(self.index, ps.DatetimeIndex):
            raise TypeError("Index must be DatetimeIndex")

        psdf = self.copy()
        psdf.index.name = verify_temp_column_name(psdf, "__index_name__")
        return_types = [psdf.index.dtype] + list(psdf.dtypes)

        def pandas_between_time(  # type: ignore[no-untyped-def]
            pdf,
        ) -> ps.DataFrame[return_types]:  # type: ignore[valid-type]
            return pdf.between_time(start_time, end_time, include_start, include_end).reset_index()

        # apply_batch will remove the index of the pandas-on-Spark DataFrame and attach a
        # default index, which will never be used. Use "distributed" index as a dummy to
        # avoid overhead.
        with option_context("compute.default_index_type", "distributed"):
            psdf = psdf.pandas_on_spark.apply_batch(pandas_between_time)

        return DataFrame(
            self._internal.copy(
                spark_frame=psdf._internal.spark_frame,
                index_spark_columns=psdf._internal.data_spark_columns[:1],
                index_fields=psdf._internal.data_fields[:1],
                data_spark_columns=psdf._internal.data_spark_columns[1:],
                data_fields=psdf._internal.data_fields[1:],
            )
        )

    # TODO: implement axis=1
    def at_time(
        self, time: Union[datetime.time, str], asof: bool = False, axis: Axis = 0
    ) -> "DataFrame":
        """
        Select values at particular time of day (example: 9:30AM).

        Parameters
        ----------
        time : datetime.time or str
        axis : {0 or 'index', 1 or 'columns'}, default 0

        Returns
        -------
        DataFrame

        Raises
        ------
        TypeError
            If the index is not  a :class:`DatetimeIndex`

        See Also
        --------
        between_time : Select values between particular times of the day.
        DatetimeIndex.indexer_at_time : Get just the index locations for
            values at particular time of the day.

        Examples
        --------
        >>> idx = pd.date_range('2018-04-09', periods=4, freq='12H')
        >>> psdf = ps.DataFrame({'A': [1, 2, 3, 4]}, index=idx)
        >>> psdf
                             A
        2018-04-09 00:00:00  1
        2018-04-09 12:00:00  2
        2018-04-10 00:00:00  3
        2018-04-10 12:00:00  4

        >>> psdf.at_time('12:00')
                             A
        2018-04-09 12:00:00  2
        2018-04-10 12:00:00  4
        """
        if asof:
            raise NotImplementedError("'asof' argument is not supported")

        axis = validate_axis(axis)

        if axis != 0:
            raise NotImplementedError("at_time currently only works for axis=0")

        if not isinstance(self.index, ps.DatetimeIndex):
            raise TypeError("Index must be DatetimeIndex")

        psdf = self.copy()
        psdf.index.name = verify_temp_column_name(psdf, "__index_name__")
        return_types = [psdf.index.dtype] + list(psdf.dtypes)

        def pandas_at_time(  # type: ignore[no-untyped-def]
            pdf,
        ) -> ps.DataFrame[return_types]:  # type: ignore[valid-type]
            return pdf.at_time(time, asof, axis).reset_index()

        # apply_batch will remove the index of the pandas-on-Spark DataFrame and attach
        # a default index, which will never be used. Use "distributed" index as a dummy
        # to avoid overhead.
        with option_context("compute.default_index_type", "distributed"):
            psdf = psdf.pandas_on_spark.apply_batch(pandas_at_time)

        return DataFrame(
            self._internal.copy(
                spark_frame=psdf._internal.spark_frame,
                index_spark_columns=psdf._internal.data_spark_columns[:1],
                index_fields=psdf._internal.data_fields[:1],
                data_spark_columns=psdf._internal.data_spark_columns[1:],
                data_fields=psdf._internal.data_fields[1:],
            )
        )

    def where(
        self,
        cond: DataFrameOrSeries,
        other: Union[DataFrameOrSeries, Any] = np.nan,
        axis: Axis = None,
    ) -> "DataFrame":
        """
        Replace values where the condition is False.

        Parameters
        ----------
        cond : boolean DataFrame
            Where cond is True, keep the original value. Where False,
            replace with corresponding value from other.
        other : scalar, DataFrame
            Entries where cond is False are replaced with corresponding value from other.
        axis : int, default None
            Can only be set to 0 now for compatibility with pandas.

        Returns
        -------
        DataFrame

        Examples
        --------

        >>> from pyspark.pandas.config import set_option, reset_option
        >>> set_option("compute.ops_on_diff_frames", True)
        >>> df1 = ps.DataFrame({'A': [0, 1, 2, 3, 4], 'B':[100, 200, 300, 400, 500]})
        >>> df2 = ps.DataFrame({'A': [0, -1, -2, -3, -4], 'B':[-100, -200, -300, -400, -500]})
        >>> df1
           A    B
        0  0  100
        1  1  200
        2  2  300
        3  3  400
        4  4  500
        >>> df2
           A    B
        0  0 -100
        1 -1 -200
        2 -2 -300
        3 -3 -400
        4 -4 -500

        >>> df1.where(df1 > 0).sort_index()
             A      B
        0  NaN  100.0
        1  1.0  200.0
        2  2.0  300.0
        3  3.0  400.0
        4  4.0  500.0

        >>> df1.where(df1 > 1, 10).sort_index()
            A    B
        0  10  100
        1  10  200
        2   2  300
        3   3  400
        4   4  500

        >>> df1.where(df1 > 1, df1 + 100).sort_index()
             A    B
        0  100  100
        1  101  200
        2    2  300
        3    3  400
        4    4  500

        >>> df1.where(df1 > 1, df2).sort_index()
           A    B
        0  0  100
        1 -1  200
        2  2  300
        3  3  400
        4  4  500

        When the column name of cond is different from self, it treats all values are False

        >>> cond = ps.DataFrame({'C': [0, -1, -2, -3, -4], 'D':[4, 3, 2, 1, 0]}) % 3 == 0
        >>> cond
               C      D
        0   True  False
        1  False   True
        2  False  False
        3   True  False
        4  False   True

        >>> df1.where(cond).sort_index()
            A   B
        0 NaN NaN
        1 NaN NaN
        2 NaN NaN
        3 NaN NaN
        4 NaN NaN

        When the type of cond is Series, it just check boolean regardless of column name

        >>> cond = ps.Series([1, 2]) > 1
        >>> cond
        0    False
        1     True
        dtype: bool

        >>> df1.where(cond).sort_index()
             A      B
        0  NaN    NaN
        1  1.0  200.0
        2  NaN    NaN
        3  NaN    NaN
        4  NaN    NaN

        >>> reset_option("compute.ops_on_diff_frames")
        """
        from pyspark.pandas.series import Series

        axis = validate_axis(axis)
        if axis != 0:
            raise NotImplementedError('axis should be either 0 or "index" currently.')

        tmp_cond_col_name = "__tmp_cond_col_{}__".format
        tmp_other_col_name = "__tmp_other_col_{}__".format

        psdf = self.copy()

        tmp_cond_col_names = [
            tmp_cond_col_name(name_like_string(label)) for label in self._internal.column_labels
        ]
        if isinstance(cond, DataFrame):
            cond = cond[
                [
                    (
                        cond._internal.spark_column_for(label)
                        if label in cond._internal.column_labels
                        else F.lit(False)
                    ).alias(name)
                    for label, name in zip(self._internal.column_labels, tmp_cond_col_names)
                ]
            ]
            psdf[tmp_cond_col_names] = cond
        elif isinstance(cond, Series):
            cond = cond.to_frame()
            cond = cond[
                [cond._internal.data_spark_columns[0].alias(name) for name in tmp_cond_col_names]
            ]
            psdf[tmp_cond_col_names] = cond
        else:
            raise TypeError("type of cond must be a DataFrame or Series")

        tmp_other_col_names = [
            tmp_other_col_name(name_like_string(label)) for label in self._internal.column_labels
        ]
        if isinstance(other, DataFrame):
            other = other[
                [
                    (
                        other._internal.spark_column_for(label)
                        if label in other._internal.column_labels
                        else F.lit(np.nan)
                    ).alias(name)
                    for label, name in zip(self._internal.column_labels, tmp_other_col_names)
                ]
            ]
            psdf[tmp_other_col_names] = other
        elif isinstance(other, Series):
            other = other.to_frame()
            other = other[
                [other._internal.data_spark_columns[0].alias(name) for name in tmp_other_col_names]
            ]
            psdf[tmp_other_col_names] = other
        else:
            for label in self._internal.column_labels:
                psdf[tmp_other_col_name(name_like_string(label))] = other

        # above logic make spark dataframe looks like below:
        # +-----------------+---+---+------------------+-------------------+------------------+--...
        # |__index_level_0__|  A|  B|__tmp_cond_col_A__|__tmp_other_col_A__|__tmp_cond_col_B__|__...
        # +-----------------+---+---+------------------+-------------------+------------------+--...
        # |                0|  0|100|              true|                  0|             false|  ...
        # |                1|  1|200|             false|                 -1|             false|  ...
        # |                3|  3|400|              true|                 -3|             false|  ...
        # |                2|  2|300|             false|                 -2|              true|  ...
        # |                4|  4|500|             false|                 -4|             false|  ...
        # +-----------------+---+---+------------------+-------------------+------------------+--...

        data_spark_columns = []
        for label in self._internal.column_labels:
            data_spark_columns.append(
                F.when(
                    psdf[tmp_cond_col_name(name_like_string(label))].spark.column,
                    psdf._internal.spark_column_for(label),
                )
                .otherwise(psdf[tmp_other_col_name(name_like_string(label))].spark.column)
                .alias(psdf._internal.spark_column_name_for(label))
            )

        return DataFrame(
            psdf._internal.with_new_columns(
                data_spark_columns, column_labels=self._internal.column_labels  # TODO: dtypes?
            )
        )

    def mask(
        self, cond: DataFrameOrSeries, other: Union[DataFrameOrSeries, Any] = np.nan
    ) -> "DataFrame":
        """
        Replace values where the condition is True.

        Parameters
        ----------
        cond : boolean DataFrame
            Where cond is False, keep the original value. Where True,
            replace with corresponding value from other.
        other : scalar, DataFrame
            Entries where cond is True are replaced with corresponding value from other.

        Returns
        -------
        DataFrame

        Examples
        --------

        >>> from pyspark.pandas.config import set_option, reset_option
        >>> set_option("compute.ops_on_diff_frames", True)
        >>> df1 = ps.DataFrame({'A': [0, 1, 2, 3, 4], 'B':[100, 200, 300, 400, 500]})
        >>> df2 = ps.DataFrame({'A': [0, -1, -2, -3, -4], 'B':[-100, -200, -300, -400, -500]})
        >>> df1
           A    B
        0  0  100
        1  1  200
        2  2  300
        3  3  400
        4  4  500
        >>> df2
           A    B
        0  0 -100
        1 -1 -200
        2 -2 -300
        3 -3 -400
        4 -4 -500

        >>> df1.mask(df1 > 0).sort_index()
             A   B
        0  0.0 NaN
        1  NaN NaN
        2  NaN NaN
        3  NaN NaN
        4  NaN NaN

        >>> df1.mask(df1 > 1, 10).sort_index()
            A   B
        0   0  10
        1   1  10
        2  10  10
        3  10  10
        4  10  10

        >>> df1.mask(df1 > 1, df1 + 100).sort_index()
             A    B
        0    0  200
        1    1  300
        2  102  400
        3  103  500
        4  104  600

        >>> df1.mask(df1 > 1, df2).sort_index()
           A    B
        0  0 -100
        1  1 -200
        2 -2 -300
        3 -3 -400
        4 -4 -500

        >>> reset_option("compute.ops_on_diff_frames")
        """
        from pyspark.pandas.series import Series

        if not isinstance(cond, (DataFrame, Series)):
            raise TypeError("type of cond must be a DataFrame or Series")

        cond_inversed = cond._apply_series_op(lambda psser: ~psser)
        return self.where(cond_inversed, other)

    @property
    def index(self) -> "Index":
        """The index (row labels) Column of the DataFrame.

        Currently not supported when the DataFrame has no index.

        See Also
        --------
        Index
        """
        from pyspark.pandas.indexes.base import Index

        return Index._new_instance(self)

    @property
    def empty(self) -> bool:
        """
        Returns true if the current DataFrame is empty. Otherwise, returns false.

        Examples
        --------
        >>> ps.range(10).empty
        False

        >>> ps.range(0).empty
        True

        >>> ps.DataFrame({}, index=list('abc')).empty
        True
        """
        return (
            len(self._internal.column_labels) == 0
            or self._internal.resolved_copy.spark_frame.rdd.isEmpty()
        )

    @property
    def style(self) -> "Styler":
        """
        Property returning a Styler object containing methods for
        building a styled HTML representation for the DataFrame.

        Examples
        --------
        >>> ps.range(1001).style  # doctest: +SKIP
        <pandas.io.formats.style.Styler object at ...>
        """
        max_results = get_option("compute.max_rows")
        if max_results is not None:
            pdf = self.head(max_results + 1)._to_internal_pandas()
            if len(pdf) > max_results:
                warnings.warn(
                    "'style' property will only use top %s rows." % max_results, UserWarning
                )
            return pdf.head(max_results).style
        else:
            return self._to_internal_pandas().style

    def set_index(
        self,
        keys: Union[Name, List[Name]],
        drop: bool = True,
        append: bool = False,
        inplace: bool = False,
    ) -> Optional["DataFrame"]:
        """Set the DataFrame index (row labels) using one or more existing columns.

        Set the DataFrame index (row labels) using one or more existing
        columns or arrays (of the correct length). The index can replace the
        existing index or expand on it.

        Parameters
        ----------
        keys : label or array-like or list of labels/arrays
            This parameter can be either a single column key, a single array of
            the same length as the calling DataFrame, or a list containing an
            arbitrary combination of column keys and arrays. Here, "array"
            encompasses :class:`Series`, :class:`Index` and ``np.ndarray``.
        drop : bool, default True
            Delete columns to be used as the new index.
        append : bool, default False
            Whether to append columns to existing index.
        inplace : bool, default False
            Modify the DataFrame in place (do not create a new object).

        Returns
        -------
        DataFrame
            Changed row labels.

        See Also
        --------
        DataFrame.reset_index : Opposite of set_index.

        Examples
        --------
        >>> df = ps.DataFrame({'month': [1, 4, 7, 10],
        ...                    'year': [2012, 2014, 2013, 2014],
        ...                    'sale': [55, 40, 84, 31]},
        ...                   columns=['month', 'year', 'sale'])
        >>> df
           month  year  sale
        0      1  2012    55
        1      4  2014    40
        2      7  2013    84
        3     10  2014    31

        Set the index to become the 'month' column:

        >>> df.set_index('month')  # doctest: +NORMALIZE_WHITESPACE
               year  sale
        month
        1      2012    55
        4      2014    40
        7      2013    84
        10     2014    31

        Create a MultiIndex using columns 'year' and 'month':

        >>> df.set_index(['year', 'month'])  # doctest: +NORMALIZE_WHITESPACE
                    sale
        year  month
        2012  1     55
        2014  4     40
        2013  7     84
        2014  10    31
        """
        inplace = validate_bool_kwarg(inplace, "inplace")
        key_list: List[Label]
        if is_name_like_tuple(keys):
            key_list = [cast(Label, keys)]
        elif is_name_like_value(keys):
            key_list = [(keys,)]
        else:
            key_list = [key if is_name_like_tuple(key) else (key,) for key in keys]
        columns = set(self._internal.column_labels)
        for key in key_list:
            if key not in columns:
                raise KeyError(name_like_string(key))

        if drop:
            column_labels = [
                label for label in self._internal.column_labels if label not in key_list
            ]
        else:
            column_labels = self._internal.column_labels
        if append:
            index_spark_columns = self._internal.index_spark_columns + [
                self._internal.spark_column_for(label) for label in key_list
            ]
            index_names = self._internal.index_names + key_list
            index_fields = self._internal.index_fields + [
                self._internal.field_for(label) for label in key_list
            ]
        else:
            index_spark_columns = [self._internal.spark_column_for(label) for label in key_list]
            index_names = key_list
            index_fields = [self._internal.field_for(label) for label in key_list]

        internal = self._internal.copy(
            index_spark_columns=index_spark_columns,
            index_names=index_names,
            index_fields=index_fields,
            column_labels=column_labels,
            data_spark_columns=[self._internal.spark_column_for(label) for label in column_labels],
            data_fields=[self._internal.field_for(label) for label in column_labels],
        )

        if inplace:
            self._update_internal_frame(internal)
            return None
        else:
            return DataFrame(internal)

    def reset_index(
        self,
        level: Optional[Union[int, Name, Sequence[Union[int, Name]]]] = None,
        drop: bool = False,
        inplace: bool = False,
        col_level: int = 0,
        col_fill: str = "",
    ) -> Optional["DataFrame"]:
        """Reset the index, or a level of it.

        For DataFrame with multi-level index, return new DataFrame with labeling information in
        the columns under the index names, defaulting to 'level_0', 'level_1', etc. if any are None.
        For a standard index, the index name will be used (if set), otherwise a default 'index' or
        'level_0' (if 'index' is already taken) will be used.

        Parameters
        ----------
        level : int, str, tuple, or list, default None
            Only remove the given levels from the index. Removes all levels by
            default.
        drop : bool, default False
            Do not try to insert index into dataframe columns. This reset
            the index to the default integer index.
        inplace : bool, default False
            Modify the DataFrame in place (do not create a new object).
        col_level : int or str, default 0
            If the columns have multiple levels, determines which level the
            labels are inserted into. By default it is inserted into the first
            level.
        col_fill : object, default ''
            If the columns have multiple levels, determines how the other
            levels are named. If None then the index name is repeated.

        Returns
        -------
        DataFrame
            DataFrame with the new index.

        See Also
        --------
        DataFrame.set_index : Opposite of reset_index.

        Examples
        --------
        >>> df = ps.DataFrame([('bird', 389.0),
        ...                    ('bird', 24.0),
        ...                    ('mammal', 80.5),
        ...                    ('mammal', np.nan)],
        ...                   index=['falcon', 'parrot', 'lion', 'monkey'],
        ...                   columns=('class', 'max_speed'))
        >>> df
                 class  max_speed
        falcon    bird      389.0
        parrot    bird       24.0
        lion    mammal       80.5
        monkey  mammal        NaN

        When we reset the index, the old index is added as a column. Unlike pandas, pandas-on-Spark
        does not automatically add a sequential index. The following 0, 1, 2, 3 are only
        there when we display the DataFrame.

        >>> df.reset_index()
            index   class  max_speed
        0  falcon    bird      389.0
        1  parrot    bird       24.0
        2    lion  mammal       80.5
        3  monkey  mammal        NaN

        We can use the `drop` parameter to avoid the old index being added as
        a column:

        >>> df.reset_index(drop=True)
            class  max_speed
        0    bird      389.0
        1    bird       24.0
        2  mammal       80.5
        3  mammal        NaN

        You can also use `reset_index` with `MultiIndex`.

        >>> index = pd.MultiIndex.from_tuples([('bird', 'falcon'),
        ...                                    ('bird', 'parrot'),
        ...                                    ('mammal', 'lion'),
        ...                                    ('mammal', 'monkey')],
        ...                                   names=['class', 'name'])
        >>> columns = pd.MultiIndex.from_tuples([('speed', 'max'),
        ...                                      ('species', 'type')])
        >>> df = ps.DataFrame([(389.0, 'fly'),
        ...                    ( 24.0, 'fly'),
        ...                    ( 80.5, 'run'),
        ...                    (np.nan, 'jump')],
        ...                   index=index,
        ...                   columns=columns)
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                       speed species
                         max    type
        class  name
        bird   falcon  389.0     fly
               parrot   24.0     fly
        mammal lion     80.5     run
               monkey    NaN    jump

        If the index has multiple levels, we can reset a subset of them:

        >>> df.reset_index(level='class')  # doctest: +NORMALIZE_WHITESPACE
                 class  speed species
                          max    type
        name
        falcon    bird  389.0     fly
        parrot    bird   24.0     fly
        lion    mammal   80.5     run
        monkey  mammal    NaN    jump

        If we are not dropping the index, by default, it is placed in the top
        level. We can place it in another level:

        >>> df.reset_index(level='class', col_level=1)  # doctest: +NORMALIZE_WHITESPACE
                        speed species
                 class    max    type
        name
        falcon    bird  389.0     fly
        parrot    bird   24.0     fly
        lion    mammal   80.5     run
        monkey  mammal    NaN    jump

        When the index is inserted under another level, we can specify under
        which one with the parameter `col_fill`:

        >>> df.reset_index(level='class', col_level=1,
        ...                col_fill='species')  # doctest: +NORMALIZE_WHITESPACE
                      species  speed species
                        class    max    type
        name
        falcon           bird  389.0     fly
        parrot           bird   24.0     fly
        lion           mammal   80.5     run
        monkey         mammal    NaN    jump

        If we specify a nonexistent level for `col_fill`, it is created:

        >>> df.reset_index(level='class', col_level=1,
        ...                col_fill='genus')  # doctest: +NORMALIZE_WHITESPACE
                        genus  speed species
                        class    max    type
        name
        falcon           bird  389.0     fly
        parrot           bird   24.0     fly
        lion           mammal   80.5     run
        monkey         mammal    NaN    jump
        """
        inplace = validate_bool_kwarg(inplace, "inplace")
        multi_index = self._internal.index_level > 1

        def rename(index: int) -> Label:
            if multi_index:
                return ("level_{}".format(index),)
            else:
                if ("index",) not in self._internal.column_labels:
                    return ("index",)
                else:
                    return ("level_{}".format(index),)

        if level is None:
            new_column_labels = [
                name if name is not None else rename(i)
                for i, name in enumerate(self._internal.index_names)
            ]
            new_data_spark_columns = [
                scol.alias(name_like_string(label))
                for scol, label in zip(self._internal.index_spark_columns, new_column_labels)
            ]
            new_data_fields = self._internal.index_fields

            index_spark_columns = []
            index_names = []
            index_fields = []
        else:
            if is_list_like(level):
                level = list(cast(Sequence[Union[int, Name]], level))
            if isinstance(level, int) or is_name_like_tuple(level):
                level_list = [cast(Union[int, Label], level)]
            elif is_name_like_value(level):
                level_list = [(level,)]
            else:
                level_list = [
                    lvl if isinstance(lvl, int) or is_name_like_tuple(lvl) else (lvl,)
                    for lvl in level
                ]

            if all(isinstance(lvl, int) for lvl in level_list):
                int_level_list = cast(List[int], level_list)
                for lev in int_level_list:
                    if lev >= self._internal.index_level:
                        raise IndexError(
                            "Too many levels: Index has only {} level, not {}".format(
                                self._internal.index_level, lev + 1
                            )
                        )
                idx = int_level_list
            elif all(is_name_like_tuple(lev) for lev in level_list):
                idx = []
                for label in cast(List[Label], level_list):
                    try:
                        i = self._internal.index_names.index(label)
                        idx.append(i)
                    except ValueError:
                        if multi_index:
                            raise KeyError("Level unknown not found")
                        else:
                            raise KeyError(
                                "Level unknown must be same as name ({})".format(
                                    name_like_string(self._internal.index_names[0])
                                )
                            )
            else:
                raise ValueError("Level should be all int or all string.")
            idx.sort()

            new_column_labels = []
            new_data_spark_columns = []
            new_data_fields = []

            index_spark_columns = self._internal.index_spark_columns.copy()
            index_names = self._internal.index_names.copy()
            index_fields = self._internal.index_fields.copy()

            for i in idx[::-1]:
                name = index_names.pop(i)
                new_column_labels.insert(0, name if name is not None else rename(i))

                scol = index_spark_columns.pop(i)
                new_data_spark_columns.insert(0, scol.alias(name_like_string(name)))

                new_data_fields.insert(0, index_fields.pop(i).copy(name=name_like_string(name)))

        if drop:
            new_data_spark_columns = []
            new_column_labels = []
            new_data_fields = []

        for label in new_column_labels:
            if label in self._internal.column_labels:
                raise ValueError("cannot insert {}, already exists".format(name_like_string(label)))

        if self._internal.column_labels_level > 1:
            column_depth = len(self._internal.column_labels[0])
            if col_level >= column_depth:
                raise IndexError(
                    "Too many levels: Index has only {} levels, not {}".format(
                        column_depth, col_level + 1
                    )
                )
            if any(col_level + len(label) > column_depth for label in new_column_labels):
                raise ValueError("Item must have length equal to number of levels.")
            new_column_labels = [
                tuple(
                    ([col_fill] * col_level)
                    + list(label)
                    + ([col_fill] * (column_depth - (len(label) + col_level)))
                )
                for label in new_column_labels
            ]

        internal = self._internal.copy(
            index_spark_columns=index_spark_columns,
            index_names=index_names,
            index_fields=index_fields,
            column_labels=new_column_labels + self._internal.column_labels,
            data_spark_columns=new_data_spark_columns + self._internal.data_spark_columns,
            data_fields=new_data_fields + self._internal.data_fields,
        )

        if inplace:
            self._update_internal_frame(internal)
            return None
        else:
            return DataFrame(internal)

    def isnull(self) -> "DataFrame":
        """
        Detects missing values for items in the current Dataframe.

        Return a boolean same-sized Dataframe indicating if the values are NA.
        NA values, such as None or numpy.NaN, gets mapped to True values.
        Everything else gets mapped to False values.

        See Also
        --------
        DataFrame.notnull

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, None), (.6, None), (.2, .1)])
        >>> df.isnull()
               0      1
        0  False  False
        1  False   True
        2  False   True
        3  False  False

        >>> df = ps.DataFrame([[None, 'bee', None], ['dog', None, 'fly']])
        >>> df.isnull()
               0      1      2
        0   True  False   True
        1  False   True  False
        """
        return self._apply_series_op(lambda psser: psser.isnull())

    isna = isnull

    def notnull(self) -> "DataFrame":
        """
        Detects non-missing values for items in the current Dataframe.

        This function takes a dataframe and indicates whether it's
        values are valid (not missing, which is ``NaN`` in numeric
        datatypes, ``None`` or ``NaN`` in objects and ``NaT`` in datetimelike).

        See Also
        --------
        DataFrame.isnull

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, None), (.6, None), (.2, .1)])
        >>> df.notnull()
              0      1
        0  True   True
        1  True  False
        2  True  False
        3  True   True

        >>> df = ps.DataFrame([['ant', 'bee', 'cat'], ['dog', None, 'fly']])
        >>> df.notnull()
              0      1     2
        0  True   True  True
        1  True  False  True
        """
        return self._apply_series_op(lambda psser: psser.notnull())

    notna = notnull

    def insert(
        self,
        loc: int,
        column: Name,
        value: Union[Scalar, "Series", Iterable],
        allow_duplicates: bool = False,
    ) -> None:
        """
        Insert column into DataFrame at specified location.

        Raises a ValueError if `column` is already contained in the DataFrame,
        unless `allow_duplicates` is set to True.

        Parameters
        ----------
        loc : int
            Insertion index. Must verify 0 <= loc <= len(columns).
        column : str, number, or hashable object
            Label of the inserted column.
        value : int, Series, or array-like
        allow_duplicates : bool, optional

        Examples
        --------
        >>> psdf = ps.DataFrame([1, 2, 3])
        >>> psdf.sort_index()
           0
        0  1
        1  2
        2  3
        >>> psdf.insert(0, 'x', 4)
        >>> psdf.sort_index()
           x  0
        0  4  1
        1  4  2
        2  4  3

        >>> from pyspark.pandas.config import set_option, reset_option
        >>> set_option("compute.ops_on_diff_frames", True)

        >>> psdf.insert(1, 'y', [5, 6, 7])
        >>> psdf.sort_index()
           x  y  0
        0  4  5  1
        1  4  6  2
        2  4  7  3

        >>> psdf.insert(2, 'z', ps.Series([8, 9, 10]))
        >>> psdf.sort_index()
           x  y   z  0
        0  4  5   8  1
        1  4  6   9  2
        2  4  7  10  3

        >>> reset_option("compute.ops_on_diff_frames")
        """
        if not isinstance(loc, int):
            raise TypeError("loc must be int")

        assert 0 <= loc <= len(self.columns)
        assert allow_duplicates is False

        if not is_name_like_value(column):
            raise TypeError(
                '"column" should be a scalar value or tuple that contains scalar values'
            )

        # TODO(SPARK-37723): Support tuple for non-MultiIndex column name.
        if is_name_like_tuple(column):
            if self._internal.column_labels_level > 1:
                if len(column) != len(self.columns.levels):  # type: ignore[attr-defined]
                    # To be consistent with pandas
                    raise ValueError('"column" must have length equal to number of column levels.')
            else:
                raise NotImplementedError(
                    "Assigning column name as tuple is only supported for MultiIndex columns "
                    "for now."
                )

        if column in self.columns:
            raise ValueError("cannot insert %s, already exists" % str(column))

        psdf = self.copy()
        psdf[column] = value
        columns = psdf.columns[:-1].insert(loc, psdf.columns[-1])
        psdf = psdf[columns]
        self._update_internal_frame(psdf._internal)

    # TODO: add frep and axis parameter
    def shift(self, periods: int = 1, fill_value: Optional[Any] = None) -> "DataFrame":
        """
        Shift DataFrame by desired number of periods.

        .. note:: the current implementation of shift uses Spark's Window without
            specifying partition specification. This leads to moving all data into
            a single partition in a single machine and could cause serious
            performance degradation. Avoid this method with very large datasets.

        Parameters
        ----------
        periods : int
            Number of periods to shift. Can be positive or negative.
        fill_value : object, optional
            The scalar value to use for newly introduced missing values.
            The default depends on the dtype of self. For numeric data, np.nan is used.

        Returns
        -------
        Copy of input DataFrame, shifted.

        Examples
        --------
        >>> df = ps.DataFrame({'Col1': [10, 20, 15, 30, 45],
        ...                    'Col2': [13, 23, 18, 33, 48],
        ...                    'Col3': [17, 27, 22, 37, 52]},
        ...                   columns=['Col1', 'Col2', 'Col3'])

        >>> df.shift(periods=3)
           Col1  Col2  Col3
        0   NaN   NaN   NaN
        1   NaN   NaN   NaN
        2   NaN   NaN   NaN
        3  10.0  13.0  17.0
        4  20.0  23.0  27.0

        >>> df.shift(periods=3, fill_value=0)
           Col1  Col2  Col3
        0     0     0     0
        1     0     0     0
        2     0     0     0
        3    10    13    17
        4    20    23    27

        """
        return self._apply_series_op(
            lambda psser: psser._shift(periods, fill_value), should_resolve=True
        )

    # TODO: axis should support 1 or 'columns' either at this moment
    def diff(self, periods: int = 1, axis: Axis = 0) -> "DataFrame":
        """
        First discrete difference of element.

        Calculates the difference of a DataFrame element compared with another element in the
        DataFrame (default is the element in the same column of the previous row).

        .. note:: the current implementation of diff uses Spark's Window without
            specifying partition specification. This leads to moving all data into
            a single partition in a single machine and could cause serious
            performance degradation. Avoid this method with very large datasets.

        Parameters
        ----------
        periods : int, default 1
            Periods to shift for calculating difference, accepts negative values.
        axis : int, default 0 or 'index'
            Can only be set to 0 now.

        Returns
        -------
        diffed : DataFrame

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 2, 3, 4, 5, 6],
        ...                    'b': [1, 1, 2, 3, 5, 8],
        ...                    'c': [1, 4, 9, 16, 25, 36]}, columns=['a', 'b', 'c'])
        >>> df
           a  b   c
        0  1  1   1
        1  2  1   4
        2  3  2   9
        3  4  3  16
        4  5  5  25
        5  6  8  36

        >>> df.diff()
             a    b     c
        0  NaN  NaN   NaN
        1  1.0  0.0   3.0
        2  1.0  1.0   5.0
        3  1.0  1.0   7.0
        4  1.0  2.0   9.0
        5  1.0  3.0  11.0

        Difference with previous column

        >>> df.diff(periods=3)
             a    b     c
        0  NaN  NaN   NaN
        1  NaN  NaN   NaN
        2  NaN  NaN   NaN
        3  3.0  2.0  15.0
        4  3.0  4.0  21.0
        5  3.0  6.0  27.0

        Difference with following row

        >>> df.diff(periods=-1)
             a    b     c
        0 -1.0  0.0  -3.0
        1 -1.0 -1.0  -5.0
        2 -1.0 -1.0  -7.0
        3 -1.0 -2.0  -9.0
        4 -1.0 -3.0 -11.0
        5  NaN  NaN   NaN
        """
        axis = validate_axis(axis)
        if axis != 0:
            raise NotImplementedError('axis should be either 0 or "index" currently.')

        return self._apply_series_op(lambda psser: psser._diff(periods), should_resolve=True)

    # TODO: axis should support 1 or 'columns' either at this moment
    def nunique(
        self,
        axis: Axis = 0,
        dropna: bool = True,
        approx: bool = False,
        rsd: float = 0.05,
    ) -> "Series":
        """
        Return number of unique elements in the object.

        Excludes NA values by default.

        Parameters
        ----------
        axis : int, default 0 or 'index'
            Can only be set to 0 now.
        dropna : bool, default True
            Don’t include NaN in the count.
        approx: bool, default False
            If False, will use the exact algorithm and return the exact number of unique.
            If True, it uses the HyperLogLog approximate algorithm, which is significantly faster
            for large amounts of data.
            Note: This parameter is specific to pandas-on-Spark and is not found in pandas.
        rsd: float, default 0.05
            Maximum estimation error allowed in the HyperLogLog algorithm.
            Note: Just like ``approx`` this parameter is specific to pandas-on-Spark.

        Returns
        -------
        The number of unique values per column as a pandas-on-Spark Series.

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 2, 3], 'B': [np.nan, 3, np.nan]})
        >>> df.nunique()
        A    3
        B    1
        dtype: int64

        >>> df.nunique(dropna=False)
        A    3
        B    2
        dtype: int64

        On big data, we recommend using the approximate algorithm to speed up this function.
        The result will be very close to the exact unique count.

        >>> df.nunique(approx=True)
        A    3
        B    1
        dtype: int64
        """
        from pyspark.pandas.series import first_series

        axis = validate_axis(axis)
        if axis != 0:
            raise NotImplementedError('axis should be either 0 or "index" currently.')
        sdf = self._internal.spark_frame.select(
            [F.lit(None).cast(StringType()).alias(SPARK_DEFAULT_INDEX_NAME)]
            + [
                self._psser_for(label)._nunique(dropna, approx, rsd)
                for label in self._internal.column_labels
            ]
        )

        # The data is expected to be small so it's fine to transpose/use the default index.
        with ps.option_context("compute.max_rows", 1):
            internal = self._internal.copy(
                spark_frame=sdf,
                index_spark_columns=[scol_for(sdf, SPARK_DEFAULT_INDEX_NAME)],
                index_names=[None],
                index_fields=[None],
                data_spark_columns=[
                    scol_for(sdf, col) for col in self._internal.data_spark_column_names
                ],
                data_fields=None,
            )
            return first_series(DataFrame(internal).transpose())

    def round(self, decimals: Union[int, Dict[Name, int], "Series"] = 0) -> "DataFrame":
        """
        Round a DataFrame to a variable number of decimal places.

        Parameters
        ----------
        decimals : int, dict, Series
            Number of decimal places to round each column to. If an int is
            given, round each column to the same number of places.
            Otherwise dict and Series round to variable numbers of places.
            Column names should be in the keys if `decimals` is a
            dict-like, or in the index if `decimals` is a Series. Any
            columns not included in `decimals` will be left as is. Elements
            of `decimals` which are not columns of the input will be
            ignored.

            .. note:: If `decimals` is a Series, it is expected to be small,
                as all the data is loaded into the driver's memory.

        Returns
        -------
        DataFrame

        See Also
        --------
        Series.round

        Examples
        --------
        >>> df = ps.DataFrame({'A':[0.028208, 0.038683, 0.877076],
        ...                    'B':[0.992815, 0.645646, 0.149370],
        ...                    'C':[0.173891, 0.577595, 0.491027]},
        ...                    columns=['A', 'B', 'C'],
        ...                    index=['first', 'second', 'third'])
        >>> df
                       A         B         C
        first   0.028208  0.992815  0.173891
        second  0.038683  0.645646  0.577595
        third   0.877076  0.149370  0.491027

        >>> df.round(2)
                   A     B     C
        first   0.03  0.99  0.17
        second  0.04  0.65  0.58
        third   0.88  0.15  0.49

        >>> df.round({'A': 1, 'C': 2})
                  A         B     C
        first   0.0  0.992815  0.17
        second  0.0  0.645646  0.58
        third   0.9  0.149370  0.49

        >>> decimals = ps.Series([1, 0, 2], index=['A', 'B', 'C'])
        >>> df.round(decimals)
                  A    B     C
        first   0.0  1.0  0.17
        second  0.0  1.0  0.58
        third   0.9  0.0  0.49
        """
        if isinstance(decimals, ps.Series):
            decimals_dict = {
                k if isinstance(k, tuple) else (k,): v
                for k, v in decimals._to_internal_pandas().items()
            }
        elif isinstance(decimals, dict):
            decimals_dict = {k if is_name_like_tuple(k) else (k,): v for k, v in decimals.items()}
        elif isinstance(decimals, int):
            decimals_dict = {k: decimals for k in self._internal.column_labels}
        else:
            raise TypeError("decimals must be an integer, a dict-like or a Series")

        def op(psser: ps.Series) -> Union[ps.Series, Column]:
            label = psser._column_label
            if label in decimals_dict:
                return F.round(psser.spark.column, decimals_dict[label])
            else:
                return psser

        return self._apply_series_op(op)

    def _mark_duplicates(
        self,
        subset: Optional[Union[Name, List[Name]]] = None,
        keep: Union[bool, str] = "first",
    ) -> Tuple[SparkDataFrame, str]:
        if subset is None:
            subset_list = self._internal.column_labels
        else:
            if is_name_like_tuple(subset):
                subset_list = [cast(Label, subset)]
            elif is_name_like_value(subset):
                subset_list = [(subset,)]
            else:
                subset_list = [sub if is_name_like_tuple(sub) else (sub,) for sub in subset]
            diff = set(subset_list).difference(set(self._internal.column_labels))
            if len(diff) > 0:
                raise KeyError(", ".join([name_like_string(d) for d in diff]))
        group_cols = [self._internal.spark_column_name_for(label) for label in subset_list]

        sdf = self._internal.resolved_copy.spark_frame

        column = verify_temp_column_name(sdf, "__duplicated__")

        if keep == "first" or keep == "last":
            if keep == "first":
                ord_func = F.asc
            else:
                ord_func = F.desc
            window = (
                Window.partitionBy(*group_cols)
                .orderBy(ord_func(NATURAL_ORDER_COLUMN_NAME))
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
            sdf = sdf.withColumn(column, F.row_number().over(window) > 1)
        elif not keep:
            window = Window.partitionBy(*group_cols).rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
            sdf = sdf.withColumn(column, F.count("*").over(window) > 1)
        else:
            raise ValueError("'keep' only supports 'first', 'last' and False")
        return sdf, column

    def duplicated(
        self,
        subset: Optional[Union[Name, List[Name]]] = None,
        keep: Union[bool, str] = "first",
    ) -> "Series":
        """
        Return boolean Series denoting duplicate rows, optionally only considering certain columns.

        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Only consider certain columns for identifying duplicates,
            default use all of the columns
        keep : {'first', 'last', False}, default 'first'
           - ``first`` : Mark duplicates as ``True`` except for the first occurrence.
           - ``last`` : Mark duplicates as ``True`` except for the last occurrence.
           - False : Mark all duplicates as ``True``.

        Returns
        -------
        duplicated : Series

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 1, 3], 'b': [1, 1, 1, 4], 'c': [1, 1, 1, 5]},
        ...                   columns = ['a', 'b', 'c'])
        >>> df
           a  b  c
        0  1  1  1
        1  1  1  1
        2  1  1  1
        3  3  4  5

        >>> df.duplicated().sort_index()
        0    False
        1     True
        2     True
        3    False
        dtype: bool

        Mark duplicates as ``True`` except for the last occurrence.

        >>> df.duplicated(keep='last').sort_index()
        0     True
        1     True
        2    False
        3    False
        dtype: bool

        Mark all duplicates as ``True``.

        >>> df.duplicated(keep=False).sort_index()
        0     True
        1     True
        2     True
        3    False
        dtype: bool
        """
        from pyspark.pandas.series import first_series

        sdf, column = self._mark_duplicates(subset, keep)

        sdf = sdf.select(
            self._internal.index_spark_columns
            + [scol_for(sdf, column).alias(SPARK_DEFAULT_SERIES_NAME)]
        )
        return first_series(
            DataFrame(
                InternalFrame(
                    spark_frame=sdf,
                    index_spark_columns=[
                        scol_for(sdf, col) for col in self._internal.index_spark_column_names
                    ],
                    index_names=self._internal.index_names,
                    index_fields=self._internal.index_fields,
                    column_labels=[None],
                    data_spark_columns=[scol_for(sdf, SPARK_DEFAULT_SERIES_NAME)],
                )
            )
        )

    # TODO: support other as DataFrame or array-like
    def dot(self, other: "Series") -> "Series":
        """
        Compute the matrix multiplication between the DataFrame and others.

        This method computes the matrix product between the DataFrame and the
        values of an other Series

        It can also be called using ``self @ other`` in Python >= 3.5.

        .. note:: This method is based on an expensive operation due to the nature
            of big data. Internally it needs to generate each row for each value, and
            then group twice - it is a huge operation. To prevent misuse, this method
            has the 'compute.max_rows' default limit of input length and raises a ValueError.

                >>> from pyspark.pandas.config import option_context
                >>> with option_context(
                ...     'compute.max_rows', 1000, "compute.ops_on_diff_frames", True
                ... ):  # doctest: +NORMALIZE_WHITESPACE
                ...     psdf = ps.DataFrame({'a': range(1001)})
                ...     psser = ps.Series([2], index=['a'])
                ...     psdf.dot(psser)
                Traceback (most recent call last):
                  ...
                ValueError: Current DataFrame's length exceeds the given limit of 1000 rows.
                Please set 'compute.max_rows' by using 'pyspark.pandas.config.set_option'
                to retrieve more than 1000 rows. Note that, before changing the
                'compute.max_rows', this operation is considerably expensive.

        Parameters
        ----------
        other : Series
            The other object to compute the matrix product with.

        Returns
        -------
        Series
            Return the matrix product between self and other as a Series.

        See Also
        --------
        Series.dot: Similar method for Series.

        Notes
        -----
        The dimensions of DataFrame and other must be compatible to
        compute the matrix multiplication. In addition, the column names of
        DataFrame and the index of other must contain the same values, as they
        will be aligned prior to the multiplication.

        The dot method for Series computes the inner product, instead of the
        matrix product here.

        Examples
        --------
        >>> from pyspark.pandas.config import set_option, reset_option
        >>> set_option("compute.ops_on_diff_frames", True)
        >>> psdf = ps.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]])
        >>> psser = ps.Series([1, 1, 2, 1])
        >>> psdf.dot(psser)
        0   -4
        1    5
        dtype: int64

        Note how shuffling of the objects does not change the result.

        >>> psser2 = psser.reindex([1, 0, 2, 3])
        >>> psdf.dot(psser2)
        0   -4
        1    5
        dtype: int64
        >>> psdf @ psser2
        0   -4
        1    5
        dtype: int64
        >>> reset_option("compute.ops_on_diff_frames")
        """
        if not isinstance(other, ps.Series):
            raise TypeError("Unsupported type {}".format(type(other).__name__))
        else:
            return cast(ps.Series, other.dot(self.transpose())).rename(None)

    def __matmul__(self, other: "Series") -> "Series":
        """
        Matrix multiplication using binary `@` operator in Python>=3.5.
        """
        return self.dot(other)

    def to_table(
        self,
        name: str,
        format: Optional[str] = None,
        mode: str = "w",
        partition_cols: Optional[Union[str, List[str]]] = None,
        index_col: Optional[Union[str, List[str]]] = None,
        **options: Any,
    ) -> None:
        if index_col is None:
            log_advice(
                "If `index_col` is not specified for `to_table`, "
                "the existing index is lost when converting to table."
            )
        mode = validate_mode(mode)
        return self.spark.to_table(name, format, mode, partition_cols, index_col, **options)

    to_table.__doc__ = SparkFrameMethods.to_table.__doc__

    def to_delta(
        self,
        path: str,
        mode: str = "w",
        partition_cols: Optional[Union[str, List[str]]] = None,
        index_col: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """
        Write the DataFrame out as a Delta Lake table.

        Parameters
        ----------
        path : str, required
            Path to write to.
        mode : str
            Python write mode, default 'w'.

            .. note:: mode can accept the strings for Spark writing mode.
                Such as 'append', 'overwrite', 'ignore', 'error', 'errorifexists'.

                - 'append' (equivalent to 'a'): Append the new data to existing data.
                - 'overwrite' (equivalent to 'w'): Overwrite existing data.
                - 'ignore': Silently ignore this operation if data already exists.
                - 'error' or 'errorifexists': Throw an exception if data already exists.

        partition_cols : str or list of str, optional, default None
            Names of partitioning columns
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default the index is always lost.
        options : dict
            All other options passed directly into Delta Lake.

        See Also
        --------
        read_delta
        DataFrame.to_parquet
        DataFrame.to_table
        DataFrame.to_spark_io

        Examples
        --------

        >>> df = ps.DataFrame(dict(
        ...    date=list(pd.date_range('2012-1-1 12:00:00', periods=3, freq='M')),
        ...    country=['KR', 'US', 'JP'],
        ...    code=[1, 2 ,3]), columns=['date', 'country', 'code'])
        >>> df
                         date country  code
        0 2012-01-31 12:00:00      KR     1
        1 2012-02-29 12:00:00      US     2
        2 2012-03-31 12:00:00      JP     3

        Create a new Delta Lake table, partitioned by one column:

        >>> df.to_delta('%s/to_delta/foo' % path, partition_cols='date')  # doctest: +SKIP

        Partitioned by two columns:

        >>> df.to_delta('%s/to_delta/bar' % path,
        ...             partition_cols=['date', 'country'])  # doctest: +SKIP

        Overwrite an existing table's partitions, using the 'replaceWhere' capability in Delta:

        >>> df.to_delta('%s/to_delta/bar' % path,
        ...             mode='overwrite', replaceWhere='date >= "2012-01-01"')  # doctest: +SKIP
        """
        if index_col is None:
            log_advice(
                "If `index_col` is not specified for `to_delta`, "
                "the existing index is lost when converting to Delta."
            )
        if "options" in options and isinstance(options.get("options"), dict) and len(options) == 1:
            options = options.get("options")  # type: ignore[assignment]

        mode = validate_mode(mode)
        self.spark.to_spark_io(
            path=path,
            mode=mode,
            format="delta",
            partition_cols=partition_cols,
            index_col=index_col,
            **options,
        )

    def to_parquet(
        self,
        path: str,
        mode: str = "w",
        partition_cols: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
        index_col: Optional[Union[str, List[str]]] = None,
        **options: Any,
    ) -> None:
        """
        Write the DataFrame out as a Parquet file or directory.

        Parameters
        ----------
        path : str, required
            Path to write to.
        mode : str
            Python write mode, default 'w'.

            .. note:: mode can accept the strings for Spark writing mode.
                Such as 'append', 'overwrite', 'ignore', 'error', 'errorifexists'.

                - 'append' (equivalent to 'a'): Append the new data to existing data.
                - 'overwrite' (equivalent to 'w'): Overwrite existing data.
                - 'ignore': Silently ignore this operation if data already exists.
                - 'error' or 'errorifexists': Throw an exception if data already exists.

        partition_cols : str or list of str, optional, default None
            Names of partitioning columns
        compression : str {'none', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'zstd'}
            Compression codec to use when saving to file. If None is set, it uses the
            value specified in `spark.sql.parquet.compression.codec`.
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default the index is always lost.
        options : dict
            All other options passed directly into Spark's data source.

        See Also
        --------
        read_parquet
        DataFrame.to_delta
        DataFrame.to_table
        DataFrame.to_spark_io

        Examples
        --------
        >>> df = ps.DataFrame(dict(
        ...    date=list(pd.date_range('2012-1-1 12:00:00', periods=3, freq='M')),
        ...    country=['KR', 'US', 'JP'],
        ...    code=[1, 2 ,3]), columns=['date', 'country', 'code'])
        >>> df
                         date country  code
        0 2012-01-31 12:00:00      KR     1
        1 2012-02-29 12:00:00      US     2
        2 2012-03-31 12:00:00      JP     3

        >>> df.to_parquet('%s/to_parquet/foo.parquet' % path, partition_cols='date')

        >>> df.to_parquet(
        ...     '%s/to_parquet/foo.parquet' % path,
        ...     mode = 'overwrite',
        ...     partition_cols=['date', 'country'])

        Notes
        -----
        pandas API on Spark writes Parquet files into the directory, `path`, and writes
        multiple part files in the directory unlike pandas.
        pandas API on Spark respects HDFS's property such as 'fs.default.name'.
        """
        if index_col is None:
            log_advice(
                "If `index_col` is not specified for `to_parquet`, "
                "the existing index is lost when converting to Parquet."
            )
        if "options" in options and isinstance(options.get("options"), dict) and len(options) == 1:
            options = options.get("options")

        mode = validate_mode(mode)
        builder = self.to_spark(index_col=index_col).write.mode(mode)
        if partition_cols is not None:
            builder.partitionBy(partition_cols)
        if compression is not None:
            builder.option("compression", compression)
        builder.options(**options).format("parquet").save(path)

    def to_orc(
        self,
        path: str,
        mode: str = "w",
        partition_cols: Optional[Union[str, List[str]]] = None,
        index_col: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """
        Write a DataFrame to the ORC format.

        Parameters
        ----------
        path : str
            Path to write to.
        mode : str
            Python write mode, default 'w'.

            .. note:: mode can accept the strings for Spark writing mode.
                Such as 'append', 'overwrite', 'ignore', 'error', 'errorifexists'.

                - 'append' (equivalent to 'a'): Append the new data to existing data.
                - 'overwrite' (equivalent to 'w'): Overwrite existing data.
                - 'ignore': Silently ignore this operation if data already exists.
                - 'error' or 'errorifexists': Throw an exception if data already exists.

        partition_cols : str or list of str, optional, default None
            Names of partitioning columns
        index_col: str or list of str, optional, default: None
            Column names to be used in Spark to represent pandas-on-Spark's index. The index name
            in pandas-on-Spark is ignored. By default the index is always lost.
        options : dict
            All other options passed directly into Spark's data source.

        See Also
        --------
        read_orc
        DataFrame.to_delta
        DataFrame.to_parquet
        DataFrame.to_table
        DataFrame.to_spark_io

        Examples
        --------
        >>> df = ps.DataFrame(dict(
        ...    date=list(pd.date_range('2012-1-1 12:00:00', periods=3, freq='M')),
        ...    country=['KR', 'US', 'JP'],
        ...    code=[1, 2 ,3]), columns=['date', 'country', 'code'])
        >>> df
                         date country  code
        0 2012-01-31 12:00:00      KR     1
        1 2012-02-29 12:00:00      US     2
        2 2012-03-31 12:00:00      JP     3

        >>> df.to_orc('%s/to_orc/foo.orc' % path, partition_cols='date')

        >>> df.to_orc(
        ...     '%s/to_orc/foo.orc' % path,
        ...     mode = 'overwrite',
        ...     partition_cols=['date', 'country'])

        Notes
        -----
        pandas API on Spark writes ORC files into the directory, `path`, and writes
        multiple part files in the directory unlike pandas.
        pandas API on Spark respects HDFS's property such as 'fs.default.name'.
        """
        if index_col is None:
            log_advice(
                "If `index_col` is not specified for `to_orc`, "
                "the existing index is lost when converting to ORC."
            )
        if "options" in options and isinstance(options.get("options"), dict) and len(options) == 1:
            options = options.get("options")  # type: ignore[assignment]

        mode = validate_mode(mode)
        self.spark.to_spark_io(
            path=path,
            mode=mode,
            format="orc",
            partition_cols=partition_cols,
            index_col=index_col,
            **options,
        )

    def to_spark_io(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        mode: str = "overwrite",
        partition_cols: Optional[Union[str, List[str]]] = None,
        index_col: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """An alias for :func:`DataFrame.spark.to_spark_io`.
        See :meth:`pyspark.pandas.spark.accessors.SparkFrameMethods.to_spark_io`.

        .. deprecated:: 3.2.0
            Use :func:`DataFrame.spark.to_spark_io` instead.
        """
        warnings.warn("Deprecated in 3.2, Use DataFrame.spark.to_spark_io instead.", FutureWarning)
        return self.spark.to_spark_io(path, format, mode, partition_cols, index_col, **options)

    to_spark_io.__doc__ = SparkFrameMethods.to_spark_io.__doc__

    def to_spark(self, index_col: Optional[Union[str, List[str]]] = None) -> SparkDataFrame:
        if index_col is None:
            log_advice(
                "If `index_col` is not specified for `to_spark`, "
                "the existing index is lost when converting to Spark DataFrame."
            )
        return self._to_spark(index_col)

    to_spark.__doc__ = SparkFrameMethods.__doc__

    def _to_spark(self, index_col: Optional[Union[str, List[str]]] = None) -> SparkDataFrame:
        """
        Same as `to_spark()`, without issuing the advice log when `index_col` is not specified
        for internal usage.
        """
        return self.spark.frame(index_col)

    def to_pandas(self) -> pd.DataFrame:
        """
        Return a pandas DataFrame.

        .. note:: This method should only be used if the resulting pandas DataFrame is expected
            to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'])
        >>> df.to_pandas()
           dogs  cats
        0   0.2   0.3
        1   0.0   0.6
        2   0.6   0.0
        3   0.2   0.1
        """
        log_advice(
            "`to_pandas` loads all data into the driver's memory. "
            "It should only be used if the resulting pandas DataFrame is expected to be small."
        )
        return self._to_pandas()

    def _to_pandas(self) -> pd.DataFrame:
        """
        Same as `to_pandas()`, without issuing the advice log for internal usage.
        """
        return self._internal.to_pandas_frame.copy()

    def assign(self, **kwargs: Any) -> "DataFrame":
        """
        Assign new columns to a DataFrame.

        Returns a new object with all original columns in addition to new ones.
        Existing columns that are re-assigned will be overwritten.

        Parameters
        ----------
        **kwargs : dict of {str: callable, Series or Index}
            The column names are keywords. If the values are
            callable, they are computed on the DataFrame and
            assigned to the new columns. The callable must not
            change input DataFrame (though pandas-on-Spark doesn't check it).
            If the values are not callable, (e.g. a Series or a literal),
            they are simply assigned.

        Returns
        -------
        DataFrame
            A new DataFrame with the new columns in addition to
            all the existing columns.

        Examples
        --------
        >>> df = ps.DataFrame({'temp_c': [17.0, 25.0]},
        ...                   index=['Portland', 'Berkeley'])
        >>> df
                  temp_c
        Portland    17.0
        Berkeley    25.0

        Where the value is a callable, evaluated on `df`:

        >>> df.assign(temp_f=lambda x: x.temp_c * 9 / 5 + 32)
                  temp_c  temp_f
        Portland    17.0    62.6
        Berkeley    25.0    77.0

        Alternatively, the same behavior can be achieved by directly
        referencing an existing Series or sequence and you can also
        create multiple columns within the same assign.

        >>> assigned = df.assign(temp_f=df['temp_c'] * 9 / 5 + 32,
        ...                      temp_k=df['temp_c'] + 273.15,
        ...                      temp_idx=df.index)
        >>> assigned[['temp_c', 'temp_f', 'temp_k', 'temp_idx']]
                  temp_c  temp_f  temp_k  temp_idx
        Portland    17.0    62.6  290.15  Portland
        Berkeley    25.0    77.0  298.15  Berkeley

        Notes
        -----
        Assigning multiple columns within the same ``assign`` is possible
        but you cannot refer to newly created or modified columns. This
        feature is supported in pandas for Python 3.6 and later but not in
        pandas-on-Spark. In pandas-on-Spark, all items are computed first,
        and then assigned.
        """
        return self._assign(kwargs)

    def _assign(self, kwargs: Any) -> "DataFrame":
        assert isinstance(kwargs, dict)
        from pyspark.pandas.indexes import MultiIndex
        from pyspark.pandas.series import IndexOpsMixin

        for k, v in kwargs.items():
            is_invalid_assignee = (
                not (isinstance(v, (IndexOpsMixin, Column)) or callable(v) or is_scalar(v))
            ) or isinstance(v, MultiIndex)
            if is_invalid_assignee:
                raise TypeError(
                    "Column assignment doesn't support type " "{0}".format(type(v).__name__)
                )
            if callable(v):
                kwargs[k] = v(self)

        pairs = {
            (k if is_name_like_tuple(k) else (k,)): (
                (v.spark.column, v._internal.data_fields[0])
                if isinstance(v, IndexOpsMixin) and not isinstance(v, MultiIndex)
                else (v, None)
                if isinstance(v, Column)
                else (F.lit(v), None)
            )
            for k, v in kwargs.items()
        }

        scols = []
        data_fields = []
        for label in self._internal.column_labels:
            for i in range(len(label)):
                if label[: len(label) - i] in pairs:
                    scol, field = pairs[label[: len(label) - i]]

                    name = self._internal.spark_column_name_for(label)
                    scol = scol.alias(name)
                    if field is not None:
                        field = field.copy(name=name)
                    break
            else:
                scol = self._internal.spark_column_for(label)
                field = self._internal.field_for(label)
            scols.append(scol)
            data_fields.append(field)

        column_labels = self._internal.column_labels.copy()
        for label, (scol, field) in pairs.items():
            if label not in set(i[: len(label)] for i in self._internal.column_labels):
                name = name_like_string(label)
                scols.append(scol.alias(name))
                if field is not None:
                    field = field.copy(name=name)
                data_fields.append(field)

                column_labels.append(label)

        level = self._internal.column_labels_level
        column_labels = [
            tuple(list(label) + ([""] * (level - len(label)))) for label in column_labels
        ]

        internal = self._internal.with_new_columns(
            scols, column_labels=column_labels, data_fields=data_fields
        )
        return DataFrame(internal)

    @staticmethod
    def from_records(
        data: Union[np.ndarray, List[tuple], dict, pd.DataFrame],
        index: Union[str, list, np.ndarray] = None,
        exclude: list = None,
        columns: list = None,
        coerce_float: bool = False,
        nrows: int = None,
    ) -> "DataFrame":
        """
        Convert structured or recorded ndarray to DataFrame.

        Parameters
        ----------
        data : ndarray (structured dtype), list of tuples, dict, or DataFrame
        index : string, list of fields, array-like
            Field of array to use as the index, alternately a specific set of input labels to use
        exclude : sequence, default None
            Columns or fields to exclude
        columns : sequence, default None
            Column names to use. If the passed data do not have names associated with them, this
            argument provides names for the columns. Otherwise this argument indicates the order of
            the columns in the result (any names not found in the data will become all-NA columns)
        coerce_float : boolean, default False
            Attempt to convert values of non-string, non-numeric objects (like decimal.Decimal) to
            floating point, useful for SQL result sets
        nrows : int, default None
            Number of rows to read if data is an iterator

        Returns
        -------
        df : DataFrame

        Examples
        --------
        Use dict as input

        >>> ps.DataFrame.from_records({'A': [1, 2, 3]})
           A
        0  1
        1  2
        2  3

        Use list of tuples as input

        >>> ps.DataFrame.from_records([(1, 2), (3, 4)])
           0  1
        0  1  2
        1  3  4

        Use NumPy array as input

        >>> ps.DataFrame.from_records(np.eye(3))
             0    1    2
        0  1.0  0.0  0.0
        1  0.0  1.0  0.0
        2  0.0  0.0  1.0
        """
        return DataFrame(
            pd.DataFrame.from_records(data, index, exclude, columns, coerce_float, nrows)
        )

    def to_records(
        self,
        index: bool = True,
        column_dtypes: Optional[Union[str, Dtype, Dict[Name, Union[str, Dtype]]]] = None,
        index_dtypes: Optional[Union[str, Dtype, Dict[Name, Union[str, Dtype]]]] = None,
    ) -> np.recarray:
        """
        Convert DataFrame to a NumPy record array.

        Index will be included as the first field of the record array if
        requested.

        .. note:: This method should only be used if the resulting NumPy ndarray is
            expected to be small, as all the data is loaded into the driver's memory.

        Parameters
        ----------
        index : bool, default True
            Include index in resulting record array, stored in 'index'
            field or using the index label, if set.
        column_dtypes : str, type, dict, default None
            If a string or type, the data type to store all columns. If
            a dictionary, a mapping of column names and indices (zero-indexed)
            to specific data types.
        index_dtypes : str, type, dict, default None
            If a string or type, the data type to store all index levels. If
            a dictionary, a mapping of index level names and indices
            (zero-indexed) to specific data types.
            This mapping is applied only if `index=True`.

        Returns
        -------
        numpy.recarray
            NumPy ndarray with the DataFrame labels as fields and each row
            of the DataFrame as entries.

        See Also
        --------
        DataFrame.from_records: Convert structured or record ndarray
            to DataFrame.
        numpy.recarray: An ndarray that allows field access using
            attributes, analogous to typed columns in a
            spreadsheet.

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 2], 'B': [0.5, 0.75]},
        ...                   index=['a', 'b'])
        >>> df
           A     B
        a  1  0.50
        b  2  0.75

        >>> df.to_records() # doctest: +SKIP
        rec.array([('a', 1, 0.5 ), ('b', 2, 0.75)],
                  dtype=[('index', 'O'), ('A', '<i8'), ('B', '<f8')])

        The index can be excluded from the record array:

        >>> df.to_records(index=False) # doctest: +SKIP
        rec.array([(1, 0.5 ), (2, 0.75)],
                  dtype=[('A', '<i8'), ('B', '<f8')])

        Specification of dtype for columns is new in pandas 0.24.0.
        Data types can be specified for the columns:

        >>> df.to_records(column_dtypes={"A": "int32"}) # doctest: +SKIP
        rec.array([('a', 1, 0.5 ), ('b', 2, 0.75)],
                  dtype=[('index', 'O'), ('A', '<i4'), ('B', '<f8')])

        Specification of dtype for index is new in pandas 0.24.0.
        Data types can also be specified for the index:

        >>> df.to_records(index_dtypes="<S2") # doctest: +SKIP
        rec.array([(b'a', 1, 0.5 ), (b'b', 2, 0.75)],
                  dtype=[('index', 'S2'), ('A', '<i8'), ('B', '<f8')])
        """
        args = locals()
        psdf = self

        return validate_arguments_and_invoke_function(
            psdf._to_internal_pandas(), self.to_records, pd.DataFrame.to_records, args
        )

    def copy(self, deep: bool = True) -> "DataFrame":
        """
        Make a copy of this object's indices and data.

        Parameters
        ----------
        deep : bool, default True
            this parameter is not supported but just dummy parameter to match pandas.

        Returns
        -------
        copy : DataFrame

        Examples
        --------
        >>> df = ps.DataFrame({'x': [1, 2], 'y': [3, 4], 'z': [5, 6], 'w': [7, 8]},
        ...                   columns=['x', 'y', 'z', 'w'])
        >>> df
           x  y  z  w
        0  1  3  5  7
        1  2  4  6  8
        >>> df_copy = df.copy()
        >>> df_copy
           x  y  z  w
        0  1  3  5  7
        1  2  4  6  8
        """
        return DataFrame(self._internal)

    def dropna(
        self,
        axis: Axis = 0,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[Name, List[Name]]] = None,
        inplace: bool = False,
    ) -> Optional["DataFrame"]:
        """
        Remove missing values.

        Parameters
        ----------
        axis : {0 or 'index'}, default 0
            Determine if rows or columns which contain missing values are
            removed.

            * 0, or 'index' : Drop rows which contain missing values.
        how : {'any', 'all'}, default 'any'
            Determine if row or column is removed from DataFrame, when we have
            at least one NA or all NA.

            * 'any' : If any NA values are present, drop that row or column.
            * 'all' : If all values are NA, drop that row or column.

        thresh : int, optional
            Require that many non-NA values.
        subset : array-like, optional
            Labels along other axis to consider, e.g. if you are dropping rows
            these would be a list of columns to include.
        inplace : bool, default False
            If True, do operation inplace and return None.

        Returns
        -------
        DataFrame
            DataFrame with NA entries dropped from it.

        See Also
        --------
        DataFrame.drop : Drop specified labels from columns.
        DataFrame.isnull: Indicate missing values.
        DataFrame.notnull : Indicate existing (non-missing) values.

        Examples
        --------
        >>> df = ps.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
        ...                    "toy": [None, 'Batmobile', 'Bullwhip'],
        ...                    "born": [None, "1940-04-25", None]},
        ...                   columns=['name', 'toy', 'born'])
        >>> df
               name        toy        born
        0    Alfred       None        None
        1    Batman  Batmobile  1940-04-25
        2  Catwoman   Bullwhip        None

        Drop the rows where at least one element is missing.

        >>> df.dropna()
             name        toy        born
        1  Batman  Batmobile  1940-04-25

        Drop the columns where at least one element is missing.

        >>> df.dropna(axis='columns')
               name
        0    Alfred
        1    Batman
        2  Catwoman

        Drop the rows where all elements are missing.

        >>> df.dropna(how='all')
               name        toy        born
        0    Alfred       None        None
        1    Batman  Batmobile  1940-04-25
        2  Catwoman   Bullwhip        None

        Keep only the rows with at least 2 non-NA values.

        >>> df.dropna(thresh=2)
               name        toy        born
        1    Batman  Batmobile  1940-04-25
        2  Catwoman   Bullwhip        None

        Define in which columns to look for missing values.

        >>> df.dropna(subset=['name', 'born'])
             name        toy        born
        1  Batman  Batmobile  1940-04-25

        Keep the DataFrame with valid entries in the same variable.

        >>> df.dropna(inplace=True)
        >>> df
             name        toy        born
        1  Batman  Batmobile  1940-04-25
        """
        axis = validate_axis(axis)
        inplace = validate_bool_kwarg(inplace, "inplace")

        if thresh is None:
            if how is None:
                raise TypeError("must specify how or thresh")
            elif how not in ("any", "all"):
                raise ValueError("invalid how option: {h}".format(h=how))

        labels: Optional[List[Label]]
        if subset is not None:
            if isinstance(subset, str):
                labels = [(subset,)]
            elif isinstance(subset, tuple):
                labels = [subset]
            else:
                labels = [sub if isinstance(sub, tuple) else (sub,) for sub in subset]
        else:
            labels = None

        if axis == 0:
            if labels is not None:
                invalids = [label for label in labels if label not in self._internal.column_labels]
                if len(invalids) > 0:
                    raise KeyError(invalids)
            else:
                labels = self._internal.column_labels

            cnt = reduce(
                lambda x, y: x + y,
                [
                    F.when(self._psser_for(label).notna().spark.column, 1).otherwise(0)
                    for label in labels
                ],
                F.lit(0),
            )
            if thresh is not None:
                pred = cnt >= F.lit(int(thresh))
            elif how == "any":
                pred = cnt == F.lit(len(labels))
            elif how == "all":
                pred = cnt > F.lit(0)

            internal = self._internal.with_filter(pred)
            if inplace:
                self._update_internal_frame(internal)
                return None
            else:
                return DataFrame(internal)
        else:
            assert axis == 1

            internal = self._internal.resolved_copy

            if labels is not None:
                if any(len(lbl) != internal.index_level for lbl in labels):
                    raise ValueError(
                        "The length of each subset must be the same as the index size."
                    )

                cond = reduce(
                    lambda x, y: x | y,
                    [
                        reduce(
                            lambda x, y: x & y,
                            [
                                scol == F.lit(part)
                                for part, scol in zip(lbl, internal.index_spark_columns)
                            ],
                        )
                        for lbl in labels
                    ],
                )

                internal = internal.with_filter(cond)

            psdf: DataFrame = DataFrame(internal)

            null_counts = []
            for label in internal.column_labels:
                psser = psdf._psser_for(label)
                cond = psser.isnull().spark.column
                null_counts.append(
                    F.sum(F.when(~cond, 1).otherwise(0)).alias(name_like_string(label))
                )

            counts = internal.spark_frame.select(null_counts + [F.count("*")]).head()

            if thresh is not None:
                column_labels = [
                    label
                    for label, cnt in zip(internal.column_labels, counts)
                    if (cnt or 0) >= int(thresh)
                ]
            elif how == "any":
                column_labels = [
                    label
                    for label, cnt in zip(internal.column_labels, counts)
                    if (cnt or 0) == counts[-1]
                ]
            elif how == "all":
                column_labels = [
                    label for label, cnt in zip(internal.column_labels, counts) if (cnt or 0) > 0
                ]

            psdf = self[column_labels]
            if inplace:
                self._update_internal_frame(psdf._internal)
                return None
            else:
                return psdf

    # TODO: add 'limit' when value parameter exists
    def fillna(
        self,
        value: Optional[Union[Any, Dict[Name, Any]]] = None,
        method: Optional[str] = None,
        axis: Optional[Axis] = None,
        inplace: bool = False,
        limit: Optional[int] = None,
    ) -> Optional["DataFrame"]:
        """Fill NA/NaN values.

        .. note:: the current implementation of 'method' parameter in fillna uses Spark's Window
            without specifying partition specification. This leads to moving all data into
            a single partition in a single machine and could cause serious
            performance degradation. Avoid this method with very large datasets.

        Parameters
        ----------
        value : scalar, dict, Series
            Value to use to fill holes. alternately a dict/Series of values
            specifying which value to use for each column.
            DataFrame is not supported.
        method : {'backfill', 'bfill', 'pad', 'ffill', None}, default None
            Method to use for filling holes in reindexed Series pad / ffill: propagate last valid
            observation forward to next valid backfill / bfill:
            use NEXT valid observation to fill gap
        axis : {0 or `index`}
            1 and `columns` are not supported.
        inplace : boolean, default False
            Fill in place (do not create a new object)
        limit : int, default None
            If method is specified, this is the maximum number of consecutive NaN values to
            forward/backward fill. In other words, if there is a gap with more than this number of
            consecutive NaNs, it will only be partially filled. If method is not specified,
            this is the maximum number of entries along the entire axis where NaNs will be filled.
            Must be greater than 0 if not None

        Returns
        -------
        DataFrame
            DataFrame with NA entries filled.

        Examples
        --------
        >>> df = ps.DataFrame({
        ...     'A': [None, 3, None, None],
        ...     'B': [2, 4, None, 3],
        ...     'C': [None, None, None, 1],
        ...     'D': [0, 1, 5, 4]
        ...     },
        ...     columns=['A', 'B', 'C', 'D'])
        >>> df
             A    B    C  D
        0  NaN  2.0  NaN  0
        1  3.0  4.0  NaN  1
        2  NaN  NaN  NaN  5
        3  NaN  3.0  1.0  4

        Replace all NaN elements with 0s.

        >>> df.fillna(0)
             A    B    C  D
        0  0.0  2.0  0.0  0
        1  3.0  4.0  0.0  1
        2  0.0  0.0  0.0  5
        3  0.0  3.0  1.0  4

        We can also propagate non-null values forward or backward.

        >>> df.fillna(method='ffill')
             A    B    C  D
        0  NaN  2.0  NaN  0
        1  3.0  4.0  NaN  1
        2  3.0  4.0  NaN  5
        3  3.0  3.0  1.0  4

        Replace all NaN elements in column 'A', 'B', 'C', and 'D', with 0, 1,
        2, and 3 respectively.

        >>> values = {'A': 0, 'B': 1, 'C': 2, 'D': 3}
        >>> df.fillna(value=values)
             A    B    C  D
        0  0.0  2.0  2.0  0
        1  3.0  4.0  2.0  1
        2  0.0  1.0  2.0  5
        3  0.0  3.0  1.0  4
        """
        axis = validate_axis(axis)
        if axis != 0:
            raise NotImplementedError("fillna currently only works for axis=0 or axis='index'")

        if value is not None:
            if not isinstance(value, (float, int, str, bool, dict, pd.Series)):
                raise TypeError("Unsupported type %s" % type(value).__name__)
            if limit is not None:
                raise ValueError("limit parameter for value is not support now")
            if isinstance(value, pd.Series):
                value = value.to_dict()
            if isinstance(value, dict):
                for v in value.values():
                    if not isinstance(v, (float, int, str, bool)):
                        raise TypeError("Unsupported type %s" % type(v).__name__)
                value = {k if is_name_like_tuple(k) else (k,): v for k, v in value.items()}

                def op(psser: ps.Series) -> ps.Series:
                    label = psser._column_label
                    for k, v in value.items():
                        if k == label[: len(k)]:
                            return psser._fillna(
                                value=value[k], method=method, axis=axis, limit=limit
                            )
                    else:
                        return psser

            else:

                def op(psser: ps.Series) -> ps.Series:
                    return psser._fillna(value=value, method=method, axis=axis, limit=limit)

        elif method is not None:

            def op(psser: ps.Series) -> ps.Series:
                return psser._fillna(value=value, method=method, axis=axis, limit=limit)

        else:
            raise ValueError("Must specify a fillna 'value' or 'method' parameter.")

        psdf = self._apply_series_op(op, should_resolve=(method is not None))

        inplace = validate_bool_kwarg(inplace, "inplace")
        if inplace:
            self._update_internal_frame(psdf._internal, check_same_anchor=False)
            return None
        else:
            return psdf

    def interpolate(
        self,
        method: str = "linear",
        limit: Optional[int] = None,
        limit_direction: Optional[str] = None,
        limit_area: Optional[str] = None,
    ) -> "DataFrame":
        if method not in ["linear"]:
            raise NotImplementedError("interpolate currently works only for method='linear'")
        if (limit is not None) and (not limit > 0):
            raise ValueError("limit must be > 0.")
        if (limit_direction is not None) and (
            limit_direction not in ["forward", "backward", "both"]
        ):
            raise ValueError("invalid limit_direction: '{}'".format(limit_direction))
        if (limit_area is not None) and (limit_area not in ["inside", "outside"]):
            raise ValueError("invalid limit_area: '{}'".format(limit_area))

        numeric_col_names = []
        for label in self._internal.column_labels:
            psser = self._psser_for(label)
            if isinstance(psser.spark.data_type, (NumericType, BooleanType)):
                numeric_col_names.append(psser.name)

        psdf = self[numeric_col_names]
        return psdf._apply_series_op(
            lambda psser: psser._interpolate(
                method=method, limit=limit, limit_direction=limit_direction, limit_area=limit_area
            ),
            should_resolve=True,
        )

    def replace(
        self,
        to_replace: Optional[Union[Any, List, Tuple, Dict]] = None,
        value: Optional[Any] = None,
        inplace: bool = False,
        limit: Optional[int] = None,
        regex: bool = False,
        method: str = "pad",
    ) -> Optional["DataFrame"]:
        """
        Returns a new DataFrame replacing a value with another value.

        Parameters
        ----------
        to_replace : int, float, string, list, tuple or dict
            Value to be replaced.
        value : int, float, string, list or tuple
            Value to use to replace holes. The replacement value must be an int, float,
            or string.
            If value is a list or tuple, value should be of the same length with to_replace.
        inplace : boolean, default False
            Fill in place (do not create a new object)

        Returns
        -------
        DataFrame
            Object after replacement.

        Examples
        --------
        >>> df = ps.DataFrame({"name": ['Ironman', 'Captain America', 'Thor', 'Hulk'],
        ...                    "weapon": ['Mark-45', 'Shield', 'Mjolnir', 'Smash']},
        ...                   columns=['name', 'weapon'])
        >>> df
                      name   weapon
        0          Ironman  Mark-45
        1  Captain America   Shield
        2             Thor  Mjolnir
        3             Hulk    Smash

        Scalar `to_replace` and `value`

        >>> df.replace('Ironman', 'War-Machine')
                      name   weapon
        0      War-Machine  Mark-45
        1  Captain America   Shield
        2             Thor  Mjolnir
        3             Hulk    Smash

        List like `to_replace` and `value`

        >>> df.replace(['Ironman', 'Captain America'], ['Rescue', 'Hawkeye'], inplace=True)
        >>> df
              name   weapon
        0   Rescue  Mark-45
        1  Hawkeye   Shield
        2     Thor  Mjolnir
        3     Hulk    Smash

        Dicts can be used to specify different replacement values for different existing values
        To use a dict in this way the value parameter should be None

        >>> df.replace({'Mjolnir': 'Stormbuster'})
              name       weapon
        0   Rescue      Mark-45
        1  Hawkeye       Shield
        2     Thor  Stormbuster
        3     Hulk        Smash

        Dict can specify that different values should be replaced in different columns
        The value parameter should not be None in this case

        >>> df.replace({'weapon': 'Mjolnir'}, 'Stormbuster')
              name       weapon
        0   Rescue      Mark-45
        1  Hawkeye       Shield
        2     Thor  Stormbuster
        3     Hulk        Smash

        Nested dictionaries
        The value parameter should be None to use a nested dict in this way

        >>> df.replace({'weapon': {'Mjolnir': 'Stormbuster'}})
              name       weapon
        0   Rescue      Mark-45
        1  Hawkeye       Shield
        2     Thor  Stormbuster
        3     Hulk        Smash
        """
        if method != "pad":
            raise NotImplementedError("replace currently works only for method='pad")
        if limit is not None:
            raise NotImplementedError("replace currently works only when limit=None")
        if regex is not False:
            raise NotImplementedError("replace currently doesn't supports regex")
        inplace = validate_bool_kwarg(inplace, "inplace")

        if value is not None and not isinstance(value, (int, float, str, list, tuple, dict)):
            raise TypeError("Unsupported type {}".format(type(value).__name__))
        if to_replace is not None and not isinstance(
            to_replace, (int, float, str, list, tuple, dict)
        ):
            raise TypeError("Unsupported type {}".format(type(to_replace).__name__))

        if isinstance(value, (list, tuple)) and isinstance(to_replace, (list, tuple)):
            if len(value) != len(to_replace):
                raise ValueError("Length of to_replace and value must be same")

        if isinstance(to_replace, dict) and (
            value is not None or all(isinstance(i, dict) for i in to_replace.values())
        ):
            to_replace_dict = to_replace

            def op(psser: ps.Series) -> ps.Series:
                if psser.name in to_replace_dict:
                    return psser.replace(
                        to_replace=to_replace_dict[psser.name], value=value, regex=regex
                    )
                else:
                    return psser

        else:

            def op(psser: ps.Series) -> ps.Series:
                return psser.replace(to_replace=to_replace, value=value, regex=regex)

        psdf = self._apply_series_op(op)
        if inplace:
            self._update_internal_frame(psdf._internal)
            return None
        else:
            return psdf

    def clip(self, lower: Union[float, int] = None, upper: Union[float, int] = None) -> "DataFrame":
        """
        Trim values at input threshold(s).

        Assigns values outside boundary-to-boundary values.

        Parameters
        ----------
        lower : float or int, default None
            Minimum threshold value. All values below this threshold will be set to it.
        upper : float or int, default None
            Maximum threshold value. All values above this threshold will be set to it.

        Returns
        -------
        DataFrame
            DataFrame with the values outside the clip boundaries replaced.

        Examples
        --------
        >>> ps.DataFrame({'A': [0, 2, 4]}).clip(1, 3)
           A
        0  1
        1  2
        2  3

        Notes
        -----
        One difference between this implementation and pandas is that running
        pd.DataFrame({'A': ['a', 'b']}).clip(0, 1) will crash with "TypeError: '<=' not supported
        between instances of 'str' and 'int'" while ps.DataFrame({'A': ['a', 'b']}).clip(0, 1)
        will output the original DataFrame, simply ignoring the incompatible types.
        """
        if is_list_like(lower) or is_list_like(upper):
            raise TypeError(
                "List-like value are not supported for 'lower' and 'upper' at the " + "moment"
            )

        if lower is None and upper is None:
            return self

        return self._apply_series_op(lambda psser: psser.clip(lower=lower, upper=upper))

    def head(self, n: int = 5) -> "DataFrame":
        """
        Return the first `n` rows.

        This function returns the first `n` rows for the object based
        on position. It is useful for quickly testing if your object
        has the right type of data in it.

        Parameters
        ----------
        n : int, default 5
            Number of rows to select.

        Returns
        -------
        obj_head : same type as caller
            The first `n` rows of the caller object.

        Examples
        --------
        >>> df = ps.DataFrame({'animal':['alligator', 'bee', 'falcon', 'lion',
        ...                    'monkey', 'parrot', 'shark', 'whale', 'zebra']})
        >>> df
              animal
        0  alligator
        1        bee
        2     falcon
        3       lion
        4     monkey
        5     parrot
        6      shark
        7      whale
        8      zebra

        Viewing the first 5 lines

        >>> df.head()
              animal
        0  alligator
        1        bee
        2     falcon
        3       lion
        4     monkey

        Viewing the first `n` lines (three in this case)

        >>> df.head(3)
              animal
        0  alligator
        1        bee
        2     falcon
        """
        if n < 0:
            n = len(self) + n
        if n <= 0:
            return DataFrame(self._internal.with_filter(F.lit(False)))
        else:
            sdf = self._internal.resolved_copy.spark_frame
            if get_option("compute.ordered_head"):
                sdf = sdf.orderBy(NATURAL_ORDER_COLUMN_NAME)
            return DataFrame(self._internal.with_new_sdf(sdf.limit(n)))

    def last(self, offset: Union[str, DateOffset]) -> "DataFrame":
        """
        Select final periods of time series data based on a date offset.

        When having a DataFrame with dates as index, this function can
        select the last few rows based on a date offset.

        Parameters
        ----------
        offset : str or DateOffset
            The offset length of the data that will be selected. For instance,
            '3D' will display all the rows having their index within the last 3 days.

        Returns
        -------
        DataFrame
            A subset of the caller.

        Raises
        ------
        TypeError
            If the index is not a :class:`DatetimeIndex`

        Examples
        --------

        >>> index = pd.date_range('2018-04-09', periods=4, freq='2D')
        >>> psdf = ps.DataFrame({'A': [1, 2, 3, 4]}, index=index)
        >>> psdf
                    A
        2018-04-09  1
        2018-04-11  2
        2018-04-13  3
        2018-04-15  4

        Get the rows for the last 3 days:

        >>> psdf.last('3D')
                    A
        2018-04-13  3
        2018-04-15  4

        Notice the data for 3 last calendar days were returned, not the last
        3 observed days in the dataset, and therefore data for 2018-04-11 was
        not returned.
        """
        # Check index type should be format DateTime
        if not isinstance(self.index, ps.DatetimeIndex):
            raise TypeError("'last' only supports a DatetimeIndex")

        from_date = cast(
            int,
            cast(datetime.datetime, self.index.max()) - cast(datetime.timedelta, to_offset(offset)),
        )

        return cast(DataFrame, self.loc[from_date:])

    def first(self, offset: Union[str, DateOffset]) -> "DataFrame":
        """
        Select first periods of time series data based on a date offset.

        When having a DataFrame with dates as index, this function can
        select the first few rows based on a date offset.

        Parameters
        ----------
        offset : str or DateOffset
            The offset length of the data that will be selected. For instance,
            '3D' will display all the rows having their index within the first 3 days.

        Returns
        -------
        DataFrame
            A subset of the caller.

        Raises
        ------
        TypeError
            If the index is not a :class:`DatetimeIndex`

        Examples
        --------

        >>> index = pd.date_range('2018-04-09', periods=4, freq='2D')
        >>> psdf = ps.DataFrame({'A': [1, 2, 3, 4]}, index=index)
        >>> psdf
                    A
        2018-04-09  1
        2018-04-11  2
        2018-04-13  3
        2018-04-15  4

        Get the rows for the last 3 days:

        >>> psdf.first('3D')
                    A
        2018-04-09  1
        2018-04-11  2

        Notice the data for 3 first calendar days were returned, not the first
        3 observed days in the dataset, and therefore data for 2018-04-13 was
        not returned.
        """
        # Check index type should be format DatetimeIndex
        if not isinstance(self.index, ps.DatetimeIndex):
            raise TypeError("'first' only supports a DatetimeIndex")

        to_date = cast(
            int,
            cast(datetime.datetime, self.index.min()) + cast(datetime.timedelta, to_offset(offset)),
        )

        return cast(DataFrame, self.loc[:to_date])

    def pivot_table(
        self,
        values: Optional[Union[Name, List[Name]]] = None,
        index: Optional[List[Name]] = None,
        columns: Optional[Name] = None,
        aggfunc: Union[str, Dict[Name, str]] = "mean",
        fill_value: Optional[Any] = None,
    ) -> "DataFrame":
        """
        Create a spreadsheet-style pivot table as a DataFrame. The levels in
        the pivot table will be stored in MultiIndex objects (hierarchical
        indexes) on the index and columns of the result DataFrame.

        Parameters
        ----------
        values : column to aggregate.
            They should be either a list less than three or a string.
        index : column (string) or list of columns
            If an array is passed, it must be the same length as the data.
            The list should contain string.
        columns : column
            Columns used in the pivot operation. Only one column is supported and
            it should be a string.
        aggfunc : function (string), dict, default mean
            If dict is passed, the key is column to aggregate and value
            is function or list of functions.
        fill_value : scalar, default None
            Value to replace missing values with.

        Returns
        -------
        table : DataFrame

        Examples
        --------
        >>> df = ps.DataFrame({"A": ["foo", "foo", "foo", "foo", "foo",
        ...                          "bar", "bar", "bar", "bar"],
        ...                    "B": ["one", "one", "one", "two", "two",
        ...                          "one", "one", "two", "two"],
        ...                    "C": ["small", "large", "large", "small",
        ...                          "small", "large", "small", "small",
        ...                          "large"],
        ...                    "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
        ...                    "E": [2, 4, 5, 5, 6, 6, 8, 9, 9]},
        ...                   columns=['A', 'B', 'C', 'D', 'E'])
        >>> df
             A    B      C  D  E
        0  foo  one  small  1  2
        1  foo  one  large  2  4
        2  foo  one  large  2  5
        3  foo  two  small  3  5
        4  foo  two  small  3  6
        5  bar  one  large  4  6
        6  bar  one  small  5  8
        7  bar  two  small  6  9
        8  bar  two  large  7  9

        This first example aggregates values by taking the sum.

        >>> table = df.pivot_table(values='D', index=['A', 'B'],
        ...                        columns='C', aggfunc='sum')
        >>> table.sort_index()  # doctest: +NORMALIZE_WHITESPACE
        C        large  small
        A   B
        bar one    4.0      5
            two    7.0      6
        foo one    4.0      1
            two    NaN      6

        We can also fill missing values using the `fill_value` parameter.

        >>> table = df.pivot_table(values='D', index=['A', 'B'],
        ...                        columns='C', aggfunc='sum', fill_value=0)
        >>> table.sort_index()  # doctest: +NORMALIZE_WHITESPACE
        C        large  small
        A   B
        bar one      4      5
            two      7      6
        foo one      4      1
            two      0      6

        We can also calculate multiple types of aggregations for any given
        value column.

        >>> table = df.pivot_table(values=['D'], index =['C'],
        ...                        columns="A", aggfunc={'D': 'mean'})
        >>> table.sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 D
        A      bar       foo
        C
        large  5.5  2.000000
        small  5.5  2.333333

        The next example aggregates on multiple values.

        >>> table = df.pivot_table(index=['C'], columns="A", values=['D', 'E'],
        ...                         aggfunc={'D': 'mean', 'E': 'sum'})
        >>> table.sort_index() # doctest: +NORMALIZE_WHITESPACE
                 D             E
        A      bar       foo bar foo
        C
        large  5.5  2.000000  15   9
        small  5.5  2.333333  17  13
        """
        if not is_name_like_value(columns):
            raise TypeError("columns should be one column name.")

        if not is_name_like_value(values) and not (
            isinstance(values, list) and all(is_name_like_value(v) for v in values)
        ):
            raise TypeError("values should be one column or list of columns.")

        if not isinstance(aggfunc, str) and (
            not isinstance(aggfunc, dict)
            or not all(
                is_name_like_value(key) and isinstance(value, str) for key, value in aggfunc.items()
            )
        ):
            raise TypeError(
                "aggfunc must be a dict mapping from column name "
                "to aggregate functions (string)."
            )

        if isinstance(aggfunc, dict) and index is None:
            raise NotImplementedError(
                "pivot_table doesn't support aggfunc" " as dict and without index."
            )
        if isinstance(values, list) and index is None:
            raise NotImplementedError("values can't be a list without index.")

        if columns not in self.columns:
            raise ValueError("Wrong columns {}.".format(name_like_string(columns)))
        if not is_name_like_tuple(columns):
            columns = (columns,)

        if isinstance(values, list):
            values = [col if is_name_like_tuple(col) else (col,) for col in values]
            if not all(
                isinstance(self._internal.spark_type_for(col), NumericType) for col in values
            ):
                raise TypeError("values should be a numeric type.")
        else:
            values = values if is_name_like_tuple(values) else (values,)
            if not isinstance(self._internal.spark_type_for(values), NumericType):
                raise TypeError("values should be a numeric type.")

        if isinstance(aggfunc, str):
            if isinstance(values, list):
                agg_cols = [
                    F.expr(
                        "{1}(`{0}`) as `{0}`".format(
                            self._internal.spark_column_name_for(value), aggfunc
                        )
                    )
                    for value in values
                ]
            else:
                agg_cols = [
                    F.expr(
                        "{1}(`{0}`) as `{0}`".format(
                            self._internal.spark_column_name_for(values), aggfunc
                        )
                    )
                ]
        elif isinstance(aggfunc, dict):
            aggfunc = {
                key if is_name_like_tuple(key) else (key,): value for key, value in aggfunc.items()
            }
            agg_cols = [
                F.expr(
                    "{1}(`{0}`) as `{0}`".format(self._internal.spark_column_name_for(key), value)
                )
                for key, value in aggfunc.items()
            ]
            agg_columns = [key for key, _ in aggfunc.items()]

            if set(agg_columns) != set(values):
                raise ValueError("Columns in aggfunc must be the same as values.")

        sdf = self._internal.resolved_copy.spark_frame
        if index is None:
            sdf = (
                sdf.groupBy()
                .pivot(pivot_col=self._internal.spark_column_name_for(columns))
                .agg(*agg_cols)
            )

        elif isinstance(index, list):
            index = [label if is_name_like_tuple(label) else (label,) for label in index]
            sdf = (
                sdf.groupBy([self._internal.spark_column_name_for(label) for label in index])
                .pivot(pivot_col=self._internal.spark_column_name_for(columns))
                .agg(*agg_cols)
            )
        else:
            raise TypeError("index should be a None or a list of columns.")

        if fill_value is not None and isinstance(fill_value, (int, float)):
            sdf = sdf.fillna(fill_value)

        psdf: DataFrame
        if index is not None:
            index_columns = [self._internal.spark_column_name_for(label) for label in index]
            index_fields = [self._internal.field_for(label) for label in index]

            if isinstance(values, list):
                data_columns = [column for column in sdf.columns if column not in index_columns]

                if len(values) > 1:
                    # If we have two values, Spark will return column's name
                    # in this format: column_values, where column contains
                    # their values in the DataFrame and values is
                    # the column list passed to the pivot_table().
                    # E.g. if column is b and values is ['b','e'],
                    # then ['2_b', '2_e', '3_b', '3_e'].

                    # We sort the columns of Spark DataFrame by values.
                    data_columns.sort(key=lambda x: x.split("_", 1)[1])
                    sdf = sdf.select(index_columns + data_columns)

                    column_name_to_index = dict(
                        zip(self._internal.data_spark_column_names, self._internal.column_labels)
                    )
                    column_labels = [
                        tuple(list(column_name_to_index[name.split("_")[1]]) + [name.split("_")[0]])
                        for name in data_columns
                    ]
                    column_label_names = (
                        [cast(Optional[Name], None)] * column_labels_level(values)
                    ) + [columns]
                    internal = InternalFrame(
                        spark_frame=sdf,
                        index_spark_columns=[scol_for(sdf, col) for col in index_columns],
                        index_names=index,
                        index_fields=index_fields,
                        column_labels=column_labels,
                        data_spark_columns=[scol_for(sdf, col) for col in data_columns],
                        column_label_names=column_label_names,
                    )
                    psdf = DataFrame(internal)
                else:
                    column_labels = [tuple(list(values[0]) + [column]) for column in data_columns]
                    column_label_names = ([cast(Optional[Name], None)] * len(values[0])) + [columns]
                    internal = InternalFrame(
                        spark_frame=sdf,
                        index_spark_columns=[scol_for(sdf, col) for col in index_columns],
                        index_names=index,
                        index_fields=index_fields,
                        column_labels=column_labels,
                        data_spark_columns=[scol_for(sdf, col) for col in data_columns],
                        column_label_names=column_label_names,
                    )
                    psdf = DataFrame(internal)
            else:
                internal = InternalFrame(
                    spark_frame=sdf,
                    index_spark_columns=[scol_for(sdf, col) for col in index_columns],
                    index_names=index,
                    index_fields=index_fields,
                    column_label_names=[columns],
                )
                psdf = DataFrame(internal)
        else:
            index_values = values
            index_map: Dict[str, Optional[Label]] = {}
            for i, index_value in enumerate(index_values):
                colname = SPARK_INDEX_NAME_FORMAT(i)
                sdf = sdf.withColumn(colname, F.lit(index_value))
                index_map[colname] = None
            internal = InternalFrame(
                spark_frame=sdf,
                index_spark_columns=[scol_for(sdf, col) for col in index_map.keys()],
                index_names=list(index_map.values()),
                column_label_names=[columns],
            )
            psdf = DataFrame(internal)

        psdf_columns = psdf.columns
        if isinstance(psdf_columns, pd.MultiIndex):
            psdf.columns = psdf_columns.set_levels(
                psdf_columns.levels[-1].astype(  # type: ignore[index]
                    spark_type_to_pandas_dtype(self._psser_for(columns).spark.data_type)
                ),
                level=-1,
            )
        else:
            psdf.columns = psdf_columns.astype(
                spark_type_to_pandas_dtype(self._psser_for(columns).spark.data_type)
            )

        return psdf

    def pivot(
        self,
        index: Optional[Name] = None,
        columns: Optional[Name] = None,
        values: Optional[Name] = None,
    ) -> "DataFrame":
        """
        Return reshaped DataFrame organized by given index / column values.

        Reshape data (produce a "pivot" table) based on column values. Uses
        unique values from specified `index` / `columns` to form axes of the
        resulting DataFrame. This function does not support data
        aggregation.

        Parameters
        ----------
        index : string, optional
            Column to use to make new frame's index. If None, uses
            existing index.
        columns : string
            Column to use to make new frame's columns.
        values : string, object or a list of the previous
            Column(s) to use for populating new frame's values.

        Returns
        -------
        DataFrame
            Returns reshaped DataFrame.

        See Also
        --------
        DataFrame.pivot_table : Generalization of pivot that can handle
            duplicate values for one index/column pair.

        Examples
        --------
        >>> df = ps.DataFrame({'foo': ['one', 'one', 'one', 'two', 'two',
        ...                            'two'],
        ...                    'bar': ['A', 'B', 'C', 'A', 'B', 'C'],
        ...                    'baz': [1, 2, 3, 4, 5, 6],
        ...                    'zoo': ['x', 'y', 'z', 'q', 'w', 't']},
        ...                   columns=['foo', 'bar', 'baz', 'zoo'])
        >>> df
           foo bar  baz zoo
        0  one   A    1   x
        1  one   B    2   y
        2  one   C    3   z
        3  two   A    4   q
        4  two   B    5   w
        5  two   C    6   t

        >>> df.pivot(index='foo', columns='bar', values='baz').sort_index()
        ... # doctest: +NORMALIZE_WHITESPACE
        bar  A  B  C
        foo
        one  1  2  3
        two  4  5  6

        >>> df.pivot(columns='bar', values='baz').sort_index()  # doctest: +NORMALIZE_WHITESPACE
        bar  A    B    C
        0  1.0  NaN  NaN
        1  NaN  2.0  NaN
        2  NaN  NaN  3.0
        3  4.0  NaN  NaN
        4  NaN  5.0  NaN
        5  NaN  NaN  6.0

        Notice that, unlike pandas raises an ValueError when duplicated values are found.
        Pandas-on-Spark's pivot still works with its first value it meets during operation because
        pivot is an expensive operation, and it is preferred to permissively execute over failing
        fast when processing large data.

        >>> df = ps.DataFrame({"foo": ['one', 'one', 'two', 'two'],
        ...                    "bar": ['A', 'A', 'B', 'C'],
        ...                    "baz": [1, 2, 3, 4]}, columns=['foo', 'bar', 'baz'])
        >>> df
           foo bar  baz
        0  one   A    1
        1  one   A    2
        2  two   B    3
        3  two   C    4

        >>> df.pivot(index='foo', columns='bar', values='baz').sort_index()
        ... # doctest: +NORMALIZE_WHITESPACE
        bar    A    B    C
        foo
        one  1.0  NaN  NaN
        two  NaN  3.0  4.0

        It also supports multi-index and multi-index column.
        >>> df.columns = pd.MultiIndex.from_tuples([('a', 'foo'), ('a', 'bar'), ('b', 'baz')])

        >>> df = df.set_index(('a', 'bar'), append=True)
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                      a   b
                    foo baz
          (a, bar)
        0 A         one   1
        1 A         one   2
        2 B         two   3
        3 C         two   4

        >>> df.pivot(columns=('a', 'foo'), values=('b', 'baz')).sort_index()
        ... # doctest: +NORMALIZE_WHITESPACE
        ('a', 'foo')  one  two
          (a, bar)
        0 A           1.0  NaN
        1 A           2.0  NaN
        2 B           NaN  3.0
        3 C           NaN  4.0

        """
        if columns is None:
            raise ValueError("columns should be set.")

        if values is None:
            raise ValueError("values should be set.")

        should_use_existing_index = index is not None
        if should_use_existing_index:
            df = self
            index_labels = [index]
        else:
            # The index after `reset_index()` will never be used, so use "distributed" index
            # as a dummy to avoid overhead.
            with option_context("compute.default_index_type", "distributed"):
                df = self.reset_index()
            index_labels = df._internal.column_labels[: self._internal.index_level]

        df = df.pivot_table(index=index_labels, columns=columns, values=values, aggfunc="first")

        if should_use_existing_index:
            return df
        else:
            internal = df._internal.copy(index_names=self._internal.index_names)
            return DataFrame(internal)

    @property
    def columns(self) -> pd.Index:
        """The column labels of the DataFrame."""
        names = [
            name if name is None or len(name) > 1 else name[0]
            for name in self._internal.column_label_names
        ]
        if self._internal.column_labels_level > 1:
            columns = pd.MultiIndex.from_tuples(self._internal.column_labels, names=names)
        else:
            columns = pd.Index([label[0] for label in self._internal.column_labels], name=names[0])
        return columns

    @columns.setter
    def columns(self, columns: Union[pd.Index, List[Name]]) -> None:
        if isinstance(columns, pd.MultiIndex):
            column_labels = columns.tolist()
        else:
            column_labels = [
                col if is_name_like_tuple(col, allow_none=False) else (col,) for col in columns
            ]

        if len(self._internal.column_labels) != len(column_labels):
            raise ValueError(
                "Length mismatch: Expected axis has {} elements, "
                "new values have {} elements".format(
                    len(self._internal.column_labels), len(column_labels)
                )
            )

        column_label_names: Optional[List]
        if isinstance(columns, pd.Index):
            column_label_names = [
                name if is_name_like_tuple(name) else (name,) for name in columns.names
            ]
        else:
            column_label_names = None

        pssers = [
            self._psser_for(label).rename(name)
            for label, name in zip(self._internal.column_labels, column_labels)
        ]
        self._update_internal_frame(
            self._internal.with_new_columns(pssers, column_label_names=column_label_names)
        )

    @property
    def dtypes(self) -> pd.Series:
        """Return the dtypes in the DataFrame.

        This returns a Series with the data type of each column. The result's index is the original
        DataFrame's columns. Columns with mixed types are stored with the object dtype.

        Returns
        -------
        pd.Series
            The data type of each column.

        Examples
        --------
        >>> df = ps.DataFrame({'a': list('abc'),
        ...                    'b': list(range(1, 4)),
        ...                    'c': np.arange(3, 6).astype('i1'),
        ...                    'd': np.arange(4.0, 7.0, dtype='float64'),
        ...                    'e': [True, False, True],
        ...                    'f': pd.date_range('20130101', periods=3)},
        ...                   columns=['a', 'b', 'c', 'd', 'e', 'f'])
        >>> df.dtypes
        a            object
        b             int64
        c              int8
        d           float64
        e              bool
        f    datetime64[ns]
        dtype: object
        """
        return pd.Series(
            [self._psser_for(label).dtype for label in self._internal.column_labels],
            index=pd.Index(
                [label if len(label) > 1 else label[0] for label in self._internal.column_labels]
            ),
        )

    def select_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
    ) -> "DataFrame":
        """
        Return a subset of the DataFrame's columns based on the column dtypes.

        Parameters
        ----------
        include, exclude : scalar or list-like
            A selection of dtypes or strings to be included/excluded. At least
            one of these parameters must be supplied. It also takes Spark SQL
            DDL type strings, for instance, 'string' and 'date'.

        Returns
        -------
        DataFrame
            The subset of the frame including the dtypes in ``include`` and
            excluding the dtypes in ``exclude``.

        Raises
        ------
        ValueError
            * If both of ``include`` and ``exclude`` are empty

                >>> df = ps.DataFrame({'a': [1, 2] * 3,
                ...                    'b': [True, False] * 3,
                ...                    'c': [1.0, 2.0] * 3})
                >>> df.select_dtypes()
                Traceback (most recent call last):
                ...
                ValueError: at least one of include or exclude must be nonempty

            * If ``include`` and ``exclude`` have overlapping elements

                >>> df = ps.DataFrame({'a': [1, 2] * 3,
                ...                    'b': [True, False] * 3,
                ...                    'c': [1.0, 2.0] * 3})
                >>> df.select_dtypes(include='a', exclude='a')
                Traceback (most recent call last):
                ...
                ValueError: include and exclude overlap on {'a'}

        Notes
        -----
        * To select datetimes, use ``np.datetime64``, ``'datetime'`` or
          ``'datetime64'``

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 2] * 3,
        ...                    'b': [True, False] * 3,
        ...                    'c': [1.0, 2.0] * 3,
        ...                    'd': ['a', 'b'] * 3}, columns=['a', 'b', 'c', 'd'])
        >>> df
           a      b    c  d
        0  1   True  1.0  a
        1  2  False  2.0  b
        2  1   True  1.0  a
        3  2  False  2.0  b
        4  1   True  1.0  a
        5  2  False  2.0  b

        >>> df.select_dtypes(include='bool')
               b
        0   True
        1  False
        2   True
        3  False
        4   True
        5  False

        >>> df.select_dtypes(include=['float64'], exclude=['int'])
             c
        0  1.0
        1  2.0
        2  1.0
        3  2.0
        4  1.0
        5  2.0

        >>> df.select_dtypes(include=['int'], exclude=['float64'])
           a
        0  1
        1  2
        2  1
        3  2
        4  1
        5  2

        >>> df.select_dtypes(exclude=['int'])
               b    c  d
        0   True  1.0  a
        1  False  2.0  b
        2   True  1.0  a
        3  False  2.0  b
        4   True  1.0  a
        5  False  2.0  b

        Spark SQL DDL type strings can be used as well.

        >>> df.select_dtypes(exclude=['string'])
           a      b    c
        0  1   True  1.0
        1  2  False  2.0
        2  1   True  1.0
        3  2  False  2.0
        4  1   True  1.0
        5  2  False  2.0
        """
        from pyspark.sql.types import _parse_datatype_string

        include_list: List[str]
        if not is_list_like(include):
            include_list = [cast(str, include)] if include is not None else []
        else:
            include_list = list(include)
        exclude_list: List[str]
        if not is_list_like(exclude):
            exclude_list = [cast(str, exclude)] if exclude is not None else []
        else:
            exclude_list = list(exclude)

        if not any((include_list, exclude_list)):
            raise ValueError("at least one of include or exclude must be " "nonempty")

        # can't both include AND exclude!
        if set(include_list).intersection(set(exclude_list)):
            raise ValueError(
                "include and exclude overlap on {inc_ex}".format(
                    inc_ex=set(include_list).intersection(set(exclude_list))
                )
            )

        # Handle Spark types
        include_spark_type = []
        for inc in include_list:
            try:
                include_spark_type.append(_parse_datatype_string(inc))
            except BaseException:
                pass

        exclude_spark_type = []
        for exc in exclude_list:
            try:
                exclude_spark_type.append(_parse_datatype_string(exc))
            except BaseException:
                pass

        # Handle pandas types
        include_numpy_type = []
        for inc in include_list:
            try:
                include_numpy_type.append(infer_dtype_from_object(inc))
            except BaseException:
                pass

        exclude_numpy_type = []
        for exc in exclude_list:
            try:
                exclude_numpy_type.append(infer_dtype_from_object(exc))
            except BaseException:
                pass

        column_labels = []
        for label in self._internal.column_labels:
            if len(include_list) > 0:
                should_include = (
                    infer_dtype_from_object(self._psser_for(label).dtype.name) in include_numpy_type
                    or self._internal.spark_type_for(label) in include_spark_type
                )
            else:
                should_include = not (
                    infer_dtype_from_object(self._psser_for(label).dtype.name) in exclude_numpy_type
                    or self._internal.spark_type_for(label) in exclude_spark_type
                )

            if should_include:
                column_labels.append(label)

        return DataFrame(
            self._internal.with_new_columns([self._psser_for(label) for label in column_labels])
        )

    def droplevel(
        self, level: Union[int, Name, List[Union[int, Name]]], axis: Axis = 0
    ) -> "DataFrame":
        """
        Return DataFrame with requested index / column level(s) removed.

        Parameters
        ----------
        level: int, str, or list-like
            If a string is given, must be the name of a level If list-like, elements must
            be names or positional indexes of levels.

        axis: {0 or ‘index’, 1 or ‘columns’}, default 0

        Returns
        -------
        DataFrame with requested index / column level(s) removed.

        Examples
        --------
        >>> df = ps.DataFrame(
        ...     [[3, 4], [7, 8], [11, 12]],
        ...     index=pd.MultiIndex.from_tuples([(1, 2), (5, 6), (9, 10)], names=["a", "b"]),
        ... )

        >>> df.columns = pd.MultiIndex.from_tuples([
        ...   ('c', 'e'), ('d', 'f')
        ... ], names=['level_1', 'level_2'])

        >>> df  # doctest: +NORMALIZE_WHITESPACE
        level_1   c   d
        level_2   e   f
        a b
        1 2      3   4
        5 6      7   8
        9 10    11  12

        >>> df.droplevel('a')  # doctest: +NORMALIZE_WHITESPACE
        level_1   c   d
        level_2   e   f
        b
        2        3   4
        6        7   8
        10      11  12

        >>> df.droplevel('level_2', axis=1)  # doctest: +NORMALIZE_WHITESPACE
        level_1   c   d
        a b
        1 2      3   4
        5 6      7   8
        9 10    11  12
        """
        axis = validate_axis(axis)
        if axis == 0:
            if not isinstance(level, (tuple, list)):  # huh?
                level = [level]

            names = self.index.names
            nlevels = self._internal.index_level

            int_level = set()
            for n in level:
                if isinstance(n, int):
                    if n < 0:
                        n = n + nlevels
                        if n < 0:
                            raise IndexError(
                                "Too many levels: Index has only {} levels, "
                                "{} is not a valid level number".format(nlevels, (n - nlevels))
                            )
                    if n >= nlevels:
                        raise IndexError(
                            "Too many levels: Index has only {} levels, not {}".format(
                                nlevels, (n + 1)
                            )
                        )
                else:
                    if n not in names:
                        raise KeyError("Level {} not found".format(n))
                    n = names.index(n)
                int_level.add(n)

            if len(level) >= nlevels:
                raise ValueError(
                    "Cannot remove {} levels from an index with {} levels: "
                    "at least one level must be left.".format(len(level), nlevels)
                )

            index_spark_columns, index_names, index_fields = zip(
                *[
                    item
                    for i, item in enumerate(
                        zip(
                            self._internal.index_spark_columns,
                            self._internal.index_names,
                            self._internal.index_fields,
                        )
                    )
      