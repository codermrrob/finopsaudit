import json
from typing import List, Dict, Any
from agno.agent import Agent
from agno.models.google import Gemini
import jsonschema

from Configuration import AgentConfig, ModelProvider, ResponseSchema
from .Models import ExtractionBatch

import logging

logger = logging.getLogger(__name__)



class LlmClient:
    """Client for interacting with the LLM via the agno library."""

    def __init__(self, model_provider: ModelProvider, temperature: float = 0.3):
        """Initializes the LLM client with settings and model configuration."""
        self.model_provider = model_provider
        self.temperature = temperature

    def process_batch(self, batch: List[str]) -> ExtractionBatch:
        """
        Formats the prompt, calls the LLM agent, and returns the structured response.

        Args:
            batch: A list of cleaned resource names to be processed.

        Returns:
            A BatchResponse object containing the extraction results.
        """
        prompt = "\n".join(batch)

        agent = Agent(
            model=Gemini(
                id=self.model_provider.value,
                temperature=self.temperature
            ),
            description=AgentConfig.AGENT_EXTRACTION_DESCRIPTION,
            instructions=AgentConfig.AGENT_EXTRACTION_INSTRUCTIONS,
            output_schema=ExtractionBatch
        )

        logger.info(f"Sending batch of {len(batch)} items to LLM. {prompt}")
        response = agent.run(prompt)
        logger.info("Received response from LLM, proceeding with validation and parsing.")

        # The `agno` agent wraps the output in a `RunOutput` object.
        # The actual structured data is in the `content` attribute.
        response_content = response.content if hasattr(response, 'content') else response

        return self._validate_and_parse_response(response_content)

    def _validate_and_parse_response(self, response_data: Any) -> ExtractionBatch:
        """
        Validates and parses the raw LLM response into an ExtractionBatch object.

        This method defensively handles various response formats:
        1. Already parsed Pydantic objects.
        2. JSON strings.
        3. Wrapped responses (e.g., `{"results": [...]}`).
        4. Unwrapped list responses (`[...]`).

        It validates the final list against the JSON schema before creating the Pydantic model.

        Args:
            response_data: The data received from the LLM.

        Returns:
            An ExtractionBatch object, which may be empty if validation fails.
        """
        if isinstance(response_data, ExtractionBatch):
            logger.info(f"LLM response is already a valid ExtractionBatch with {len(response_data.results)} items.")
            return response_data

        data_to_validate = None
        try:
            # If response is a string, parse it as JSON
            if isinstance(response_data, str):
                response_data = json.loads(response_data)

            # Handle wrapped vs. unwrapped responses
            if isinstance(response_data, list):
                data_to_validate = response_data
            elif isinstance(response_data, dict) and len(response_data) == 1 and isinstance(list(response_data.values())[0], list):
                key = list(response_data.keys())[0]
                logger.info(f"Unwrapping response from root key: '{key}'")
                data_to_validate = list(response_data.values())[0]
            else:
                logger.warning(f"Unsupported response structure: {type(response_data)}. Expected list or single-key dict.")
                return ExtractionBatch(results=[])

            # Validate the unwrapped data against the schema
            jsonschema.validate(instance=data_to_validate, schema=ResponseSchema.SCHEMA)
            logger.info(f"LLM response passed JSON schema validation with {len(data_to_validate)} items.")

            # If validation is successful, construct the Pydantic model
            return ExtractionBatch(results=data_to_validate)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode LLM response as JSON: {e}")
        except jsonschema.ValidationError as e:
            logger.error(f"LLM response failed schema validation: {e.message}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during response parsing: {e}")

        return ExtractionBatch(results=[])
