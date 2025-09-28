from typing import List, Tuple, Optional, Callable

from Configuration import AgentConfig, ModelProvider

from .Models import ExtractionResult
from .LlmClient import LlmClient
import logging

logger = logging.getLogger(__name__)

class EntityExtractor:
    """
    Cloud resource entity extractor with configurable batch processing.
    """

    def __init__(
        self,
        model_provider: ModelProvider = ModelProvider.GEMINI_FLASH,
        batch_size_override: Optional[int] = None,
        temperature: float = 0.3,
    ):
        """
        Initializes the extractor with model configuration and the LLM client.

        Args:
            model_provider: The model provider to use for extraction.
            batch_size_override: Optional override for the calculated batch size.
            temperature: The temperature setting for the LLM.
        """
        self.model_provider = model_provider
        self.config = AgentConfig.get_model_config(self.model_provider)
        logger.info(f"Selected model config: {self.config.model_dump()}")
        self.batch_size = batch_size_override or self.config.recommended_batch_size
        self.llm_client = LlmClient(model_provider, temperature)

    def process(
        self,
        resources: List[Tuple[str, str]],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> List[ExtractionResult]:
        """
        Processes a list of cloud resources in optimized batches.

        Args:
            resources: A list of tuples, each containing (original_name, cleaned_name).
            progress_callback: An optional function to report progress (current_batch, total_batches).

        Returns:
            A list of ExtractionResult objects.
        """
        total = len(resources)
        all_results: List[ExtractionResult] = []

        if self.batch_size <= 0:
            logger.warning("Batch size is zero or negative. Skipping processing.")
            return all_results

        logger.info(f"Processing {total} resources in batches of {self.batch_size} using {self.model_provider.value}")

        for i in range(0, total, self.batch_size):
            batch = resources[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total + self.batch_size - 1) // self.batch_size
            logger.info(f"Starting LLM analysis for batch {batch_num}/{total_batches}.")

            if progress_callback:
                progress_callback(batch_num, total_batches)

            batch_results = self._process_batch(batch)
            all_results.extend(batch_results)

        return all_results

    def _process_batch(self, batch: List[Tuple[str, str]]) -> List[ExtractionResult]:
        """
        Processes a single batch of resources using the LLM client.
        """
        if not batch:
            return []

        # Create a map from clean_name to original_name for easy lookup.
        name_map = {clean: orig for orig, clean in batch}
        cleaned_names = list(name_map.keys())

        try:
            response = self.llm_client.process_batch(cleaned_names)
            logger.debug(f"Raw response from LLM: {response}")

            num_entities_returned = sum(len(res.entities) for res in response.results)
            logger.info(f"LLM returned {num_entities_returned} entities for this batch.")

            # Process results and map them back using the chunk.
            results_map = {res.chunk: res.entities for res in response.results}
            
            batch_results: List[ExtractionResult] = []
            for clean_name, original_name in name_map.items():
                entities = results_map.get(clean_name, [])
                if clean_name not in results_map:
                    logger.warning(f"Missing LLM result for item: {original_name} (clean: {clean_name})")
                
                batch_results.append(
                    ExtractionResult(original_name=original_name, clean_name=clean_name, entities=entities)
                )
            
            return batch_results

        except Exception as e:
            logger.error(f"Batch processing failed with error: {e}")
            # On failure, return empty results for this batch to not halt the entire process.
            return [
                ExtractionResult(original_name=orig, clean_name=clean, entities=[])
                for orig, clean in batch
            ]
