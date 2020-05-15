"""
    Module contain all type definition

"""
from airflow.lineage.backend.atlas.additional_attributes import additonal_operator_attributes
from airflow.lineage.datasets import StandardAirflowOperator, StandardTable, StandardColumn, StandardFile

TABLE_COLUMN_RELATIONSHIP_TYPE = "standard_table_column"  # relationship name


def to_string_attribute(name) -> dict:
    """
    Generate optional string attribute definition

    :param name: Name of attribute
    :type str
    :return: Attribute definition for rest response
    :type dict
    """
    return {
        "name": name,
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    }


operator_attributes_defs = [
    {
        "name": "dag_id",
        "isOptional": False,
        "isUnique": False,
        "isIndexable": True,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "task_id",
        "isOptional": False,
        "isUnique": False,
        "isIndexable": True,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "command",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "conn_id",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "last_execution_date",
        "isOptional": False,
        "isUnique": False,
        "isIndexable": True,
        "typeName": "date",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "start_date",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "date",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "end_date",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "date",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "template_fields",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    }
]

"""
    Generate additional operator attributes, these attributes taken from constants package
"""
for attr in additonal_operator_attributes:
    operator_attributes_defs.append(to_string_attribute(attr))

operator_typedef = {
    "enumDefs": [],
    "structDefs": [],
    "classificationDefs": [],
    "entityDefs": [
        {
            "superTypes": [
                "Process"
            ],
            "name": StandardAirflowOperator.type_name,
            "description": "Airflow standard Operator",
            "createdBy": "airflow",
            "updatedBy": "airflow",
            "attributeDefs": operator_attributes_defs
        }
    ]
}

standard_table_type = {
    "superTypes": ["DataSet"],
    "name": StandardTable.type_name,
    "description": "Standard table",
    "attributeDefs": [
        {
            "name": "schema_name",
            "typeName": "string"
        },
        {
            "name": "table_name",
            "typeName": "string"
        }
    ],
    "options": {
        "schemaElementsAttribute": "columns"
    },
    "relationshipAttributeDefs": [
        {
            "name": "columns",
            "typeName": "array<{}>".format(StandardColumn.type_name),
            "isOptional": True,
            "cardinality": "SET",
            "valuesMinCount": -1,
            "valuesMaxCount": -1,
            "isUnique": False,
            "isIndexable": False,
            "includeInNotification": False,
            "relationshipTypeName": TABLE_COLUMN_RELATIONSHIP_TYPE,
            "isLegacyAttribute": True
        }
    ]
}

standard_column_type = {
    "superTypes": ["DataSet"],
    "name": StandardColumn.type_name,
    "description": "Redshift column",
    "attributeDefs": [
        {
            "name": "column",
            "typeName": "string"
        },
        {
            "name": "type",
            "typeName": "string"
        },
        {
            "name": "encoding",
            "typeName": "string"
        },
        {
            "name": "distkey",
            "typeName": "boolean"
        },
        {
            "name": "sortkey",
            "typeName": "int"
        },
        {
            "name": "notnull",
            "typeName": "boolean"
        }
    ],
    "options": {
        "schemaAttributes": "[\"column\","
                            " \"type\","
                            " \"encoding\","
                            " \"distkey\","
                            " \"sortkey\","
                            " \"notnull\"]"
    },
    "relationshipAttributeDefs": [
        {
            "name": "table",
            "typeName": StandardTable.type_name,
            "isOptional": False,
            "cardinality": "SINGLE",
            "valuesMinCount": -1,
            "valuesMaxCount": -1,
            "isUnique": False,
            "isIndexable": False,
            "includeInNotification": False,
            "relationshipTypeName": TABLE_COLUMN_RELATIONSHIP_TYPE,
            "isLegacyAttribute": True
        },
    ]

}

standard_table_column_relationship_type = {
    "name": TABLE_COLUMN_RELATIONSHIP_TYPE,
    "description": "Standard table to standard column relationship",
    "serviceType": "abstract_source",
    "relationshipCategory": "COMPOSITION",
    "relationshipLabel": "__{}.columns".format(StandardTable.type_name),
    "endDef1": {
        "type": StandardTable.type_name,
        "name": "columns",
        "isContainer": True,
        "cardinality": "SET",
        "isLegacyAttribute": True
    },
    "endDef2": {
        "type": StandardColumn.type_name,
        "name": "table",
        "isContainer": False,
        "cardinality": "SINGLE",
        "isLegacyAttribute": True
    }
}

standard_file_type = {
    "superTypes": ["DataSet"],
    "name": StandardFile.type_name,
    "description": "Standard file",
    "attributeDefs": [
        {
            "name": "path",
            "typeName": "string"
        },
        {
            "name": "cluster_name",
            "typeName": "string"
        }
    ]
}

classifications_typedef = {
    "enumDefs": [],
    "structDefs": [],
    "entityDefs": [],
    "classificationDefs": [
        {
            "name": "Standard_entities",
            "description": "All standard entities",
            "superTypes": [],
            "attributeDefs": []
        },
        {
            "name": "Airflow_operators",
            "description": "All airflow operators",
            "superTypes": [],
            "attributeDefs": []
        }
    ]
}
