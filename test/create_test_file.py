import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


json_schema = """
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()

reader = DataFileReader(open("users.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{"namespace": "example2.avro",
 "type": "int",
 "name": "my_int"
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("root-int.avro", "wb"), DatumWriter(), schema)
writer.append(42)
writer.append(43)

writer.close()

reader = DataFileReader(open("root-int.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
 "fields": [
     {"name": "single_union", "type": ["int"]}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("single-union.avro", "wb"), DatumWriter(), schema)
writer.append({"single_union": 42})


writer.close()

reader = DataFileReader(open("single-union.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
 "fields": [
     {"name": "null_first", "type": ["null","int"]}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("null_first.avro", "wb"), DatumWriter(), schema)
writer.append({"null_first": 42})
writer.append({})


writer.close()

reader = DataFileReader(open("null_first.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
 "fields": [
     {"name": "null_last", "type": ["int","null"]}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("null_last.avro", "wb"), DatumWriter(), schema)
writer.append({"null_last": 42})
writer.append({})


writer.close()

reader = DataFileReader(open("null_last.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
 "fields": [
     {"name": "null", "type": "null"},
     {"name": "boolean", "type": "boolean"},
     {"name": "int", "type": "int"},
     {"name": "long", "type": "long"},
     {"name": "float", "type": "float"},
     {"name": "double", "type": "double"},
     {"name": "bytes", "type": "bytes"},
     {"name": "string", "type": "string"}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("primitive_types.avro", "wb"), DatumWriter(), schema)


writer.append(
    {
        'null': None,
        'boolean': False,
        'int': -2147483648,
        'long': -9223372036854775808,
        'float': -3.4028235e38,
        'double': -1.7976931348623157e308,
        'bytes': 'thisisalongblob\x00withnullbytes'.encode(),
        'string': "🦆🦆🦆🦆🦆🦆",
    }
)
writer.append(
    {
        'null': None,
        'boolean': True,
        'int': 2147483647,
        'long': 9223372036854775807,
        'float': 3.4028235e38,
        'double': 1.7976931348623157e308,
        'bytes': '\x00\x00\x00a'.encode(),
        'string': 'goo',
    }
)


writer.close()

reader = DataFileReader(open("primitive_types.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{
    "type": "record",
    "name": "MySchema",
    "namespace": "com.company",
    "fields": [
        {
            "name": "color",
            "type": {
                "type": "enum",
                "name": "Color",
                "symbols": [
                    "UNKNOWN",
                    "GREEN",
                    "RED"
                ]
            },
            "default": "UNKNOWN"
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("enum.avro", "wb"), DatumWriter(), schema)


writer.append({'color': 'GREEN'})
writer.append({'color': 'GREEN'})
writer.append({'color': 'RED'})
writer.append({'color': 'UNKNOWN'})
writer.append({'color': 'UNKNOWN'})

writer.close()

reader = DataFileReader(open("enum.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
     "fields": [
        {
            "name": "md5",
            "type": {"type": "fixed", "size": 32, "name": "md5"}
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("fixed.avro", "wb"), DatumWriter(), schema)


writer.append({'md5': '47336f3f2497b70ac046cf23298e20a7'.encode()})
writer.append({'md5': 'a789a15a7ff7db4a0d1b186363ef0771'.encode()})
writer.append({'md5': 'c9db7c67a6acb5a65c78b19e9e01d7b0'.encode()})
writer.append({'md5': 'ac441296bcbd44442301204a8f061cf2'.encode()})


writer.close()

reader = DataFileReader(open("fixed.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
     "fields": [
        {
            "name": "string_arr",
            "type": {
              "type": "array",
              "items" : "string",
              "default": []
            }
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("string_array.avro", "wb"), DatumWriter(), schema)


writer.append({'string_arr': ['Hello', 'World']})
writer.append({'string_arr': ['this']})
writer.append({'string_arr': []})
writer.append({'string_arr': ['is', 'cool', 'array']})
writer.append({'string_arr': ['data']})


writer.close()

reader = DataFileReader(open("string_array.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
     "fields": [
        {
            "name": "long_map",
            "type":  {
               "type": "map",
               "values" : "long",
               "default": {}
             }
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("long_map.avro", "wb"), DatumWriter(), schema)

writer.append({'long_map': {'one': 42}})
writer.append({'long_map': {'two': 43}})
writer.append({'long_map': {'three': 44}})

writer.close()

reader = DataFileReader(open("long_map.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
     "fields": [
        {
            "name": "string_arr",
            "type": ["null", {
              "type": "array",
              "items" : "string",
              "default": []
            }]
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("nullable_string_array.avro", "wb"), DatumWriter(), schema)


writer.append({'string_arr': ['Hello', 'World']})
writer.append({'string_arr': ['this']})
writer.append({'string_arr': []})
writer.append({'string_arr': None})
writer.append({'string_arr': None})
writer.append({'string_arr': ['is', 'cool', 'array']})
writer.append({'string_arr': ['data']})


writer.close()

reader = DataFileReader(open("nullable_string_array.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
     "fields": [
        {
            "name": "string_arr",
            "type": {
              "type": "array",
              "items" : ["string", "null"],
              "default": []
            }
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("nullable_entry_string_array.avro", "wb"), DatumWriter(), schema)


writer.append({'string_arr': ['Hello', None, 'World']})
writer.append({'string_arr': ['this']})
writer.append({'string_arr': [None]})
writer.append({'string_arr': [None, None, None]})
writer.append({'string_arr': []})
writer.append({'string_arr': [None, 'is', 'cool', None, 'array', None]})
writer.append({'string_arr': ['data', None]})


writer.close()

reader = DataFileReader(open("nullable_entry_string_array.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
     "fields": [
        {
            "name": "string_arr",
            "type": ["null", {
              "type": "array",
              "items" : ["string", "null"],
              "default": []
            }]
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("all_nullable_list.avro", "wb"), DatumWriter(), schema)


writer.append({'string_arr': ['Hello', None, 'World']})
writer.append({'string_arr': ['this']})
writer.append({'string_arr': [None]})
writer.append({'string_arr': [None, None, None]})
writer.append({'string_arr': []})
writer.append({'string_arr': None})
writer.append({'string_arr': None})
writer.append({'string_arr': [None, 'is', 'cool', None, 'array', None]})
writer.append({'string_arr': ['data', None]})


writer.close()

reader = DataFileReader(open("all_nullable_list.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
     "fields": [
        {
            "name": "nested_ints",
            "type": ["null", {
              "type": "array",
              "items" : ["null", {
                  "type": "array",
                  "items" : ["int", "null"],
                  "default": []
                }],
              "default": []
            }]
        }
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("nested_nullable_lists.avro", "wb"), DatumWriter(), schema)


writer.append({'nested_ints': None})
writer.append({'nested_ints': [None]})
writer.append({'nested_ints': [[None], [None]]})
writer.append({'nested_ints': [None, None]})
writer.append({'nested_ints': [[42]]})
writer.append({'nested_ints': [[42], [43]]})
writer.append({'nested_ints': [[42, 43]]})
writer.append({'nested_ints': [[42, 43], None, [44, 45]]})
writer.append({'nested_ints': [[42, None, 43, None], None, [44, None, 45, None], None, [46]]})

writer.close()

reader = DataFileReader(open("nested_nullable_lists.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "value", "type": "long"},          
    {"name": "next", "type": ["null", "LongList"]}
  ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("recursive.avro", "wb"), DatumWriter(), schema)


writer.append({'value': 42})
writer.append({'value': 43, 'next': {'value': 44}})
writer.append({'value': 43, 'next': {'value': 44, 'next': {'value': 45}}})

writer.close()

reader = DataFileReader(open("recursive.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
"name": "root",
     "fields": [
    {"name": "n", "type": "null"}          
    ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("broken_record.avro", "wb"), DatumWriter(), schema)

writer.append({})
writer.append({})

# writer.append({ 'value': 42})
# writer.append({ 'value': 43, 'next' : {'value': 44}})
# writer.append({ 'value': 43, 'next' : {'value': 44, 'next' : {'value': 45}}})

writer.close()

reader = DataFileReader(open("broken_record.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


# record
# detect recursive types or what happens here?


# union by name


json_schema = """
{ "type": "record",
 "name": "root",
 "fields": [
     {"name": "one", "type": "int"},
     {"name": "two", "type": "double"},
     {"name": "three", "type": "string"}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("union-name-1.avro", "wb"), DatumWriter(), schema)


writer.append({'one': 10, 'two': 2.0, 'three': 's30'})
writer.append({'one': 11, 'two': 2.1, 'three': 's31'})


writer.close()

reader = DataFileReader(open("union-name-1.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
 "fields": [
      {"name": "two", "type": "double"},
     {"name": "one", "type": "int"},
     {"name": "three", "type": "string"}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("union-name-2.avro", "wb"), DatumWriter(), schema)


writer.append({'one': 12, 'two': 2.2, 'three': 's32'})
writer.append({'one': 13, 'two': 2.3, 'three': 's33'})


writer.close()

reader = DataFileReader(open("union-name-2.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{ "type": "record",
 "name": "root",
 "fields": [
      {"name": "three", "type": "string"},
      {"name": "two", "type": "double"},
     {"name": "one", "type": "int"}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("union-name-3.avro", "wb"), DatumWriter(), schema)


writer.append({'one': 14, 'two': 2.4, 'three': 's34'})
writer.append({'one': 15, 'two': 2.5, 'three': 's35'})


writer.close()

reader = DataFileReader(open("union-name-3.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{
  "type": "record",
  "name": "Request",
  "namespace": "example.avro",
  "fields": [
    {
      "name": "request_id",
      "type": "string"
    },
    {
      "name": "client_version",
      "type": {
        "type": "record",
        "name": "Version",
        "fields": [
          {
            "name": "major",
            "type": "int"
          },
          {
            "name": "minor",
            "type": "int"
          }
        ]
      }
    },
    {
      "name": "server_version",
      "type": "Version"
    }
  ]
}
"""


schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("reuse-1.avro", "wb"), DatumWriter(), schema)


writer.append(
    {'request_id': 'hello', 'client_version': {'major': 4, 'minor': 2}, 'server_version': {'major': 8, 'minor': 5}}
)
writer.append(
    {'request_id': 'world', 'client_version': {'major': 5, 'minor': 3}, 'server_version': {'major': 9, 'minor': 6}}
)


writer.close()

reader = DataFileReader(open("reuse-1.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{
  "type": "record",
  "name": "Request",
  "namespace": "example.avro",
  "fields": [
    {
      "name": "version",
      "type": {
        "type": "record",
        "name": "Version",
        "fields": [
          { "name": "major", "type": "int" },
          { "name": "minor", "type": "int" }
        ]
      }
    },
    {
      "name": "details",
      "type": {
        "type": "record",
        "name": "Details",
        "fields": [
          { "name": "release_version", "type": "Version" }
        ]
      }
    }
  ]
}
"""


schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("reuse-2.avro", "wb"), DatumWriter(), schema)


writer.append({'version': {'major': 4, 'minor': 2}, 'details': {'release_version': {'major': 8, 'minor': 5}}})
writer.append({'version': {'major': 5, 'minor': 3}, 'details': {'release_version': {'major': 9, 'minor': 6}}})


writer.close()

reader = DataFileReader(open("reuse-2.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()


json_schema = """
{"type": "record",
 "name": "root",
 "fields": [
      {"name": "c0", "type": "long"},
      {"name": "c1", "type": "long"},
      {"name": "c2", "type": "long"},
      {"name": "c3", "type": "long"},
      {"name": "c4", "type": "long"},
      {"name": "c5", "type": "long"},
      {"name": "c6", "type": "long"},
      {"name": "c7", "type": "long"},
      {"name": "c8", "type": "long"},
      {"name": "c9", "type": "long"}
 ]
}
"""

n = 100000

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("bigdata.avro", "wb"), DatumWriter(), schema, codec="deflate")
for r in range(1000000):
    writer.append({f'c{i}': 10000000 * i + r for i in range(10)})

writer.close()


count = 0
reader = DataFileReader(open("bigdata.avro", "rb"), DatumReader())
for user in reader:
    count = count + 1
reader.close()
print(count)
