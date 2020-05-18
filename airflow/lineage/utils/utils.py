"""
    Module contains common function for create different dataset atlas entities.
"""
from airflow.lineage.datasets import DataSet, StandardTable, StandardFile

dataset_unique_key = "qualified_name"  # If key is represent, atlas dataset entity will be created, see StandardDataSet
standard_table_unique_key = "full_table_name"  # If key is represent, atlas standard table entity will be created, see StandardTable
standard_file_unique_key = "full_file_path"  # If key is represent, atlas standard file entity will be created, see StandardFile
redshift_table_unique_key = "redshift_full_table_name"  # If key is represent, atlas redshift table entity will be created, see RedshiftTable


def transform_to_atlas_dataset_entity(qualified_name: str) -> DataSet:
    """
    Create Atlas Dataset entity object from qualifed name
    :param qualified_name: Atlas dataset full qualified name, should be unique. For table, it's full table name, for files it's full path.
    :return: Atlas DataSet entity object
    :type: StandardDataSet
    """
    return DataSet(qualified_name=qualified_name, data={"name": qualified_name})


def __split_full_table_name(full_table_name: str) -> dict:
    """
    Split full table name into schema and table name
    :param full_table_name: Full table name with schema
    :return: dictionary, what contains schema name and table name in corresponding key
    :type: dict()
    """
    schema_name, table_name = full_table_name.split(".")
    return {
        "schema_name": schema_name,
        "table_name": table_name
    }


def transform_to_standard_table_entity(full_table_name: str):
    """
    Create Atlas StandardTable entity object from full table name
    :param full_table_name: Full table name with schema
    :return: Atlas Standard Table entity object
    :type: StandardTable
    """
    table_info = __split_full_table_name(full_table_name)
    return StandardTable(qualified_name=full_table_name, data={
        "name": full_table_name,
        "schema_name": table_info['schema_name'],
        "table_name": table_info['table_name']})


def transform_to_file_entity(full_path: str, cluster_name="none"):
    """
    Create Atlas StandardFile entity object from full file path
    :param full_path: Full file path
    :type full_path: str
    :param cluster_name: Cluster name, where file is exists, can be missing
    :type cluster_name: str
    :return: Atlas Standard file entity object
    :type: StandardFile
    """
    return StandardFile(qualified_name="{}@{}".format(full_path, cluster_name), data={
        "name": full_path.split("/")[-1],
        "path": full_path,
        "cluster_name": cluster_name
    })


def create_atlas_entities(entities) -> dict:
    """
    Create dictionary with 1 key "dataset" with correct list of atlas entities
    :param entities: Atlas Entity in dict format or list of these entities
    :type entities: Union[list[dict], dict]
    :return: Dataset dictionary in correct format for airflow operator. Is used in inlets or outlets attributes
    :type: dict
    """
    datasets = list()
    if isinstance(entities, list):
        for entity in entities:
            datasets.append(create_atlas_entity(entity))
    else:
        datasets.append(create_atlas_entity(entities))
    return {"datasets": datasets}


def create_atlas_entity(entity: dict):
    """
    Depends on attribute in dict, Create different atlas entity object
    :param entity: Dict with attributes for creating corresponding atlas entity.
    :type entity: dict
    :return: Atlas entity object
    :type: DataSet
    """

    if dataset_unique_key in entity:
        return transform_to_atlas_dataset_entity(entity.get(dataset_unique_key))
    if standard_table_unique_key in entity:
        return transform_to_standard_table_entity(entity.get(standard_table_unique_key))
    if standard_file_unique_key in entity:
        return transform_to_file_entity(entity.get(standard_file_unique_key), entity.get("cluster_name", "none"))


def create_atlas_specific_type_entities(unique_key, values):
    result = list()
    if isinstance(values, list):
        for value in values:
            result.append({unique_key: value})
    else:
        result.append({unique_key: values})
    return create_atlas_entities(result)


def create_atlas_dataset_entities(values):
    return create_atlas_specific_type_entities(dataset_unique_key, values)


def create_atlas_standard_file_entities(values):
    return create_atlas_specific_type_entities(standard_file_unique_key, values)


def create_atlas_standard_table_entities(values):
    return create_atlas_specific_type_entities(standard_table_unique_key, values)


def create_atlas_redshift_table_entities(values):
    return create_atlas_specific_type_entities(redshift_table_unique_key, values)
