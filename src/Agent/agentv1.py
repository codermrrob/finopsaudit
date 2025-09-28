import os
from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError, RootModel, field_validator
from Configuration import AgentConfig
from agno.agent import Agent
from agno.models.google import Gemini
from google.api_core import exceptions as google_exceptions
from google.api_core.retry import Retry
import logging

logger = logging.getLogger(__name__)


# --- Pydantic v2 Schemas ---

class ExtractedEntity(BaseModel):
    """Represents a single identified entity and its acronyms."""
    entity: str = Field(
        ...,
        description="A single identified business entity name.",
        min_length=1,
        examples=["nespresso", "billing", "office365"]
    )
    acronyms: List[str] = Field(
        ...,
        description="A list of corresponding acronyms for the entity.",
        min_length=1,
        max_length=5
    )
    
    @field_validator('acronyms')
    @classmethod
    def validate_acronyms(cls, v: List[str]) -> List[str]:
        """Ensure each acronym is 3-5 characters."""
        for acronym in v:
            if not 3 <= len(acronym) <= 5:
                raise ValueError(f"Acronym '{acronym}' must be 3-5 characters")
        return v
    
    # Pydantic v2 configuration
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "entity": "nespresso",
                    "acronyms": ["nes", "nsp", "nspr"]
                }
            ]
        }
    }

class CandidateList(RootModel[List[ExtractedEntity]]):
    """A root model to hold a list of extracted entities."""
    root: List[ExtractedEntity]  # Explicitly define root for v2
    
    def __iter__(self):
        """Allow iteration over the root list."""
        return iter(self.root)
    
    def __len__(self):
        """Return length of the root list."""
        return len(self.root)

class FinalCandidate(BaseModel):
    """Represents the final output for a single resource."""
    original_resource_name: str
    extracted_entities: List[ExtractedEntity]
    
    # Pydantic v2 configuration
    model_config = {
        "validate_assignment": True  # Validate on assignment
    }

# --- Agent Class ---

class EntityExtractionAgent:
    """A wrapper around an Agno agent for entity extraction."""

    def __init__(
        self, 
        model_name: str = "gemini-2.5-flash", 
        temperature: float = 1.0
    ):
        """
        Initializes the EntityExtractionAgent.

        Args:
            model_name: The name of the Gemini model to use.
            temperature: The temperature for the LLM.
        """
        if not os.getenv("GOOGLE_API_KEY"):
            logger.warning("GOOGLE_API_KEY environment variable not set. LLM calls may fail.")

        # Define a retry policy for transient network errors
        retry_policy = Retry(
            predicate=google_exceptions.RetryError.if_transient,
            initial=1.0,  # Initial delay in seconds
            maximum=60.0, # Maximum delay in seconds
            multiplier=2.0, # Factor to increase delay by
            deadline=300.0, # Total time to keep retrying
        )

        self.agno_agent = Agent(
            model=Gemini(
                id=model_name, 
                temperature=temperature,
                timeout=120,  # Set a 120-second timeout for each request
                retry=retry_policy # Apply the retry policy
            ),
            description=AgentConfig.AGENT_EXTRACTION_DESCRIPTION,
            instructions=AgentConfig.AGENT_EXTRACTION_INSTRUCTIONS,
            output_schema=CandidateList
        )

    def run(self, residues_with_origin: List[tuple[str, str]]) -> List[FinalCandidate]:
        """
        Processes a list of residues with the LLM to get entity suggestions.

        Args:
            residues_with_origin: A list of (original_name, residue) tuples.

        Returns:
            A list of final candidates including original names.
        """
        all_final_candidates = []
        
        for original_name, residue in residues_with_origin:
            logger.info(f"Calling LLM with resource name chunk: '{residue}'")
            
            try:
                response_model = self.agno_agent.run(residue)
                
                # Handle the response based on what agno returns
                if response_model and response_model.content:
                    # If content is a CandidateList (RootModel)
                    if isinstance(response_model.content, CandidateList):
                        extracted_entities = response_model.content.root
                    # If content is already a list
                    elif isinstance(response_model.content, list):
                        extracted_entities = response_model.content
                    else:
                        # Try to access root attribute
                        extracted_entities = getattr(response_model.content, 'root', [])
                    
                    if extracted_entities:
                        final_candidate = FinalCandidate(
                            original_resource_name=original_name,
                            extracted_entities=extracted_entities
                        )
                        all_final_candidates.append(final_candidate)
                        logger.info(
                            f"Received {len(extracted_entities)} entities "
                            f"for residue '{residue}'."
                        )
                        
            except KeyboardInterrupt:
                logger.warning("Process interrupted by user. Exiting...")
                break
            except ValidationError as e:
                logger.error(f"Pydantic validation failed for residue '{residue}': {e}")
                # Could also access e.errors() for detailed error info in v2
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred while processing "
                    f"residue '{residue}': {e}"
                )
        
        return all_final_candidates