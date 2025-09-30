"""
Contains the EntityNameSimilarity class for finding and grouping similar entity names.
"""

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import unicodedata
import numpy as np
from collections import defaultdict
import networkx as nx
import logging
import time

from Configuration import SimilarityConfig

logger = logging.getLogger(__name__)

class EntityNameSimilarity:
    """Analyzes a list of entity names to find and group similar ones."""

    def __init__(self, entities: list[dict]):
        """Initializes the analyzer with a list of entity objects."""
        self.entities = entities
        self.names = [e[SimilarityConfig.ENTITY_NAME_KEY] for e in self.entities]

    def _normalize(self, s: str) -> str:
        """Applies NFKC normalization, lowercasing, and stripping."""
        return unicodedata.normalize("NFKC", s).lower().strip()


    def _build_vectors(self):
        """Builds TF-IDF vectors from the entity names."""
        texts = [self._normalize(x) for x in self.names]
        vectorizer = TfidfVectorizer(analyzer="char", ngram_range=SimilarityConfig.NGRAM_RANGE)
        return vectorizer.fit_transform(texts)

    def _find_reciprocal_topk_links(self) -> list[tuple[int, int, float, str]]:
        """Finds strong, mutual links between entities.

        Performs a full pairwise comparison and filters for reciprocal top-K links.
        """
        logger.info("  3a: Building TF-IDF vectors...")
        X = self._build_vectors()

        logger.info("  3b: Calculating full cosine similarity matrix...")
        # This computes the similarity of all pairs. For 5k items, this is a 5k x 5k matrix.
        sim_matrix = cosine_similarity(X)
        logger.info(f"    Calculated similarity for {sim_matrix.shape[0]}x{sim_matrix.shape[1]} matrix.")

        neighbors = defaultdict(list)
        # Iterate over the upper triangle of the matrix to get all unique pairs.
        for i in range(sim_matrix.shape[0]):
            for j in range(i + 1, sim_matrix.shape[1]):
                s = sim_matrix[i, j]
                # Pre-filter to avoid storing millions of low-similarity pairs
                if s >= SimilarityConfig.RELATED_THRESHOLD:
                    neighbors[i].append((j, s))
                    neighbors[j].append((i, s))

        logger.info("  3c: Filtering for reciprocal top-K links...")
        topk = {}
        for i, lst in neighbors.items():
            topk[i] = lst[:SimilarityConfig.RECIPROCAL_TOP_K]

        edges = []
        for i, lst in topk.items():
            for j, s in lst:
                # A link is formed if:
                # A) It's a reciprocal top-K link (high-quality).
                # OR
                # B) The similarity is above the high-confidence threshold (catches non-reciprocal but obvious links).
                is_reciprocal = i in {x for x, _ in topk.get(j, [])}
                is_high_confidence = s >= SimilarityConfig.CONFIDENCE_THRESHOLD

                if is_reciprocal or is_high_confidence:
                    if s >= SimilarityConfig.DUPLICATE_THRESHOLD:
                        edges.append((i, j, s, SimilarityConfig.DUPLICATE_VALUE))
                    elif s >= SimilarityConfig.RELATED_THRESHOLD:
                        edges.append((i, j, s, SimilarityConfig.RELATED_VALUE))
        
        seen = set()
        keep = []
        for i, j, s, t in edges:
            a, b = sorted((i, j))
            if (a, b) not in seen:
                seen.add((a, b))
                keep.append((a, b, s, t))
        return keep

    def _create_groups_from_links(self, edges: list) -> list[dict]:
        """Builds a graph from links and extracts connected components as groups."""
        logger.info("  3e: Building graph and finding connected components...")
        G = nx.Graph()
        G.add_nodes_from(range(len(self.names)))
        for i, j, s, t in edges:
            G.add_edge(i, j, similarity=s, link_type=t)

        groups = []
        for comp in nx.connected_components(G):
            if len(comp) < 2:
                continue
            
            comp = list(comp)
            deg = {i: G.degree(i) for i in comp}
            avg_sim = {i: np.mean([G[i][nbr]['similarity'] for nbr in G.neighbors(i)]) if G.degree(i) > 0 else 0.0 for i in comp}

            candidates = sorted(comp, key=lambda i: (-deg[i], -avg_sim[i], self.names[i]))
            canonical_idx = candidates[0]

            members = []
            for member_idx in comp:
                if member_idx == canonical_idx:
                    continue
                
                if G.has_edge(canonical_idx, member_idx):
                    sim = G[canonical_idx][member_idx][SimilarityConfig.SIMILARITY_KEY]
                    link_type = G[canonical_idx][member_idx][SimilarityConfig.LINK_TYPE_KEY]
                else:
                    # Find the best similarity from the member to any other node in the component
                    sim = max((G[member_idx][nbr][SimilarityConfig.SIMILARITY_KEY] for nbr in G.neighbors(member_idx)), default=0.0)
                    link_type = SimilarityConfig.RELATED_VALUE # Default if not directly connected to canonical

                members.append({
                    SimilarityConfig.ENTITY_NAME_KEY: self.names[member_idx],
                    SimilarityConfig.SIMILARITY_KEY: round(float(sim), 3),
                    SimilarityConfig.LINK_TYPE_KEY: link_type
                })
            
            groups.append({
                SimilarityConfig.CANONICAL_KEY: self.names[canonical_idx],
                SimilarityConfig.MEMBERS_KEY: members
            })
        return groups

    def find_similar_groups(self) -> list[dict]:
        """Orchestrates the full similarity analysis and returns the groups."""
        if len(self.names) < 2:
            logger.info("Skipping similarity analysis: requires at least 2 entities.")
            return []
        
        start_time = time.perf_counter()
        
        links = self._find_reciprocal_topk_links()
        logger.info(f"    Found {len(links)} high-confidence, mutual links.")
        
        groups = self._create_groups_from_links(links)
        logger.info(f"    Collapsed links into {len(groups)} distinct entity groups.")
        
        end_time = time.perf_counter()
        elapsed_ms = (end_time - start_time) * 1000
        logger.info(f"Similarity analysis completed in {elapsed_ms:.2f} ms.")
        
        return groups
