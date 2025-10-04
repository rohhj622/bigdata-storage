import datetime
import pyarrow as pa
import pandas as pd
import duckdb
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema, NestedField, StringType, TimestamptzType
from pyiceberg.transforms import IdentityTransform, YearTransform, MonthTransform
from pyiceberg.types import IntegerType, FloatType, LongType, DoubleType, TimestampType

CATALOG_PROPERTIES = {
    'uri': 'http://localhost:8181',
    's3.endpoint': 'http://localhost:9000',
    's3.access-key-id': 'minioadmin',
    's3.secret-access-key': 'minioadmin'
}

catalog = load_catalog('aimsys', **CATALOG_PROPERTIES)

try:
    catalog.create_namespace(namespace='weather')
except NamespaceAlreadyExistsError:
    print("namespace already exists")

table_identifier = f'weather.interpolated'

schemas = Schema(
    NestedField(1, 'stn', IntegerType(), required=False),
    NestedField(2, 'stn_type', IntegerType(), required=False),
    NestedField(3, 'lon', DoubleType(), required=False),
    NestedField(4, 'lat', DoubleType(), required=False),
    NestedField(5, 'tm', TimestampType(), required=False),
    NestedField(6, 'ta', DoubleType(), required=False),
    NestedField(7, 'ca_tot', DoubleType(), required=False),
    NestedField(8, 'hm', DoubleType(), required=False),
    NestedField(9, 'rn', DoubleType(), required=False),
    NestedField(10, 'ss', DoubleType(), required=False),
    NestedField(11, 'si', DoubleType(), required=False),
    NestedField(12, 'wd', DoubleType(), required=False),
    NestedField(13, 'ws', DoubleType(), required=False),
)

partition_spec = PartitionSpec(
    # 1순위: 날짜 (년도)
    PartitionField(
        source_id=5,      # tm의 field_id
        field_id=1001, 
        name='year_pt', 
        transform=YearTransform()
    ),
    # 2순위: 날짜 (월)
    PartitionField(
        source_id=5,      # tm의 field_id
        field_id=1002, 
        name='month_pt', 
        transform=MonthTransform()
    ),
    # 3순위: 관측소
    PartitionField(
        source_id=1,      # stn의 field_id
        field_id=1003, 
        name='stn_pt', 
        transform=IdentityTransform()
    ),
)

try:
    # catalog.drop_table(table_identifier)
    table = catalog.create_table(table_identifier, schema=schemas, partition_spec=partition_spec)
except TableAlreadyExistsError:
    table = catalog.load_table(table_identifier)
    print("Table already exists")