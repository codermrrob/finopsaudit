# File: config_loader.py
import yaml
from pathlib import Path
from typing import Union
from Configuration import AuditConfig
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    def __init__(self, settings: AuditConfig):
        self.settings = settings

    def load_exclusions(self, file_path: Union[str, Path]) -> set[str]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                exclusions = set(content.split())
            logger.info(f"Loaded {len(exclusions)} exclusions from {file_path}")
            return exclusions
        except FileNotFoundError:
            logger.warning(f"Exclusion file not found: {file_path}. Returning empty set.")
            return set()
        except Exception as e:
            logger.error(f"Error loading exclusion file {file_path}: {e}")
            return set()

    def load_environments(self, file_path: Union[str, Path]) -> set[str]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                exclusions = set(content.split())
            logger.info(f"Loaded {len(exclusions)} environments from {file_path}")
            return exclusions
        except FileNotFoundError:
            logger.warning(f"environments file not found: {file_path}. Returning empty set.")
            return set()
        except Exception as e:
            logger.error(f"Error loading environments file {file_path}: {e}")
            return set()
    
    def load_regions(self, file_path: Union[str, Path]) -> set[str]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                exclusions = set(content.split())
            logger.info(f"Loaded {len(exclusions)} regions from {file_path}")
            return exclusions
        except FileNotFoundError:
            logger.warning(f"regions file not found: {file_path}. Returning empty set.")
            return set()
        except Exception as e:
            logger.error(f"Error loading regions file {file_path}: {e}")
            return set()    


    def load_known_entities(self, file_path: Union[str, Path]) -> list[dict]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                root = yaml.safe_load(f)
            if not isinstance(root, dict):
                logger.warning(f"Known entities YAML {file_path} root is not a dict: {type(root)}")
                return []
            data = root.get(AuditConfig.KNOWN_ENTITIES_YAML_KEY, [])
            if not isinstance(data, list):
                logger.warning(f"Key '{AuditConfig.KNOWN_ENTITIES_YAML_KEY}' is not a list in {file_path}")
                return []
            validated = []
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    validated.append(item)
                else:
                    logger.warning(f"Item at index {i} is not a dict: {item}")
            logger.info(f"Successfully loaded {len(validated)} known entities from {file_path}")
            return validated
        except FileNotFoundError:
            logger.error(f"Known entities file not found: {file_path}")
            return []
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {file_path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading known entities {file_path}: {e}")
            return []
