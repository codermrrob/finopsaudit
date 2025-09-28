from typing import List
from pydantic import BaseModel, Field


class BusinessEntity(BaseModel):
    """A single business entity with its generated abbreviations."""
    entity_name: str = Field(..., description="The name of the business entity.")
    abbreviations: List[str] = Field(
        ..., 
        description="A list of generated abbreviations for the entity."
    )


class EntityExtractionResult(BaseModel):
    """Extraction result for a single input chunk."""
    chunk: str = Field(..., description="The original input string for which entities were extracted.")
    entities: List[BusinessEntity] = Field(
        default_factory=list,
        description="List of business entities extracted from the chunk."
    )


class ExtractionBatch(BaseModel):
    """Defines the schema for a batch of extraction results from the LLM."""
    results: List[EntityExtractionResult] = Field(
        ...,
        description="A list of extraction results, one for each chunk in the input batch."
    )


class ExtractionResult(BaseModel):
    """Final result for a resource, including original and clean names."""
    original_name: str
    clean_name: str
    entities: List[BusinessEntity]
