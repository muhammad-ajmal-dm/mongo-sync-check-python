import pymongo
import json
from typing import Dict, List, Any
import logging
from datetime import datetime
from deepdiff import DeepDiff
from bson import ObjectId
from deepdiff.model import SetOrdered

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle ObjectId and SetOrdered."""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, SetOrdered):
            return list(obj)  # Convert SetOrdered to list
        return super().default(obj)

def make_hashable(obj: Any) -> Any:
    """Recursively convert unhashable types (lists, dicts) to hashable types (tuples), preserving order."""
    if isinstance(obj, dict):
        return tuple((k, make_hashable(v)) for k, v in sorted(obj.items()))
    elif isinstance(obj, list):
        return tuple(make_hashable(item) for item in obj)  # Preserve list order
    elif isinstance(obj, set):
        return tuple(make_hashable(item) for item in sorted(obj))
    elif isinstance(obj, ObjectId):
        return str(obj)  # Convert ObjectId to string for hashing
    return obj

def load_config(config_path: str) -> Dict:
    """Load configuration from a JSON file."""
    try:
        with open(config_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        raise

def connect_to_db(db_config: Dict) -> pymongo.database.Database:
    """Connect to a MongoDB database."""
    try:
        client = pymongo.MongoClient(
            host=db_config['host'],
            port=db_config.get('port', 27017),
            username=db_config.get('username'),
            password=db_config.get('password'),
            authSource=db_config.get('authSource', 'admin')
        )
        return client[db_config['db_name']]
    except Exception as e:
        logger.error(f"Error connecting to database {db_config['db_name']}: {e}")
        raise

def get_collection_data(db: pymongo.database.Database, collection_name: str, exclude_fields: List[str]) -> List[Dict]:
    """Fetch all documents from a collection, including _id but excluding specified fields."""
    projection = {field: 0 for field in exclude_fields}
    try:
        return list(db[collection_name].find({}, projection))
    except Exception as e:
        logger.error(f"Error fetching data from collection {collection_name}: {e}")
        raise

def compare_collections(
    source_data: List[Dict],
    target_data: List[Dict],
    collection_name: str,
    exclude_fields: List[str]
) -> Dict[str, Any]:
    """Compare two collections using _id as the unique key and identify content differences."""
    try:
        # Create dictionaries mapping _id to documents, converting _id to string
        source_dict = {str(doc['_id']): {k: v for k, v in doc.items() if k not in exclude_fields + ['_id']} for doc in source_data}
        target_dict = {str(doc['_id']): {k: v for k, v in doc.items() if k not in exclude_fields + ['_id']} for doc in target_data}

        # Identify _ids present in each collection
        source_ids = set(source_dict.keys())
        target_ids = set(target_dict.keys())

        # Find documents missing in each collection
        missing_in_source_ids = target_ids - source_ids
        missing_in_target_ids = source_ids - target_ids
        common_ids = source_ids & target_ids

        # Convert missing documents to output format, including _id as string
        missing_in_source = [{'_id': _id, **target_dict[_id]} for _id in missing_in_source_ids]
        missing_in_target = [{'_id': _id, **source_dict[_id]} for _id in missing_in_target_ids]

        # Compare content of documents with matching _ids
        content_differences = []
        for _id in common_ids:
            src_doc = source_dict[_id]
            tgt_doc = target_dict[_id]
            diff = DeepDiff(src_doc, tgt_doc, ignore_order=False)
            if diff:
                content_differences.append({
                    '_id': _id,
                    'source_doc': src_doc,
                    'target_doc': tgt_doc,
                    'diff': diff.to_dict()
                })

        differences = {
            'missing_in_source': missing_in_source,
            'missing_in_target': missing_in_target,
            'common_count': len(common_ids),
            'content_differences': content_differences
        }

        logger.info(f"Comparison results for {collection_name}:")
        logger.info(f"Documents missing in source (present in target): {len(differences['missing_in_source'])}")
        logger.info(f"Documents missing in target (present in source): {len(differences['missing_in_target'])}")
        logger.info(f"Common documents: {differences['common_count']}")
        if content_differences:
            logger.info(f"Content differences for common _ids: {json.dumps(content_differences, indent=2, cls=JSONEncoder)}")

        return differences
    except Exception as e:
        logger.error(f"Error comparing collections {collection_name}: {e}")
        raise

def main():
    # Load configuration
    config = load_config('config.json')

    # Extract database configurations
    source_db_config = config['source_db']
    target_db_config = config['target_db']
    collections = config['collections']

    # Connect to databases
    source_db = connect_to_db(source_db_config)
    target_db = connect_to_db(target_db_config)

    # Compare each collection
    for collection in collections:
        collection_name = collection['name']
        exclude_fields = collection.get('exclude_fields', [])
        logger.info(f"Comparing collection: {collection_name} (Excluding fields: {exclude_fields})")
        source_data = get_collection_data(source_db, collection_name, exclude_fields)
        target_data = get_collection_data(target_db, collection_name, exclude_fields)
        differences = compare_collections(source_data, target_data, collection_name, exclude_fields)

        # Save differences to a file
        with open(f'differences_{collection_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
            json.dump(
                differences,
                f,
                indent=2,
                cls=JSONEncoder
            )

if __name__ == "__main__":
    main()