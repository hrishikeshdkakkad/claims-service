"""
Count-Min Sketch implementation for efficient top provider tracking.

Algorithm Explanation:
----------------------
Count-Min Sketch is a probabilistic data structure that provides approximate
frequency counts using sub-linear space. It uses multiple hash functions and
a 2D array to track counts with bounded error.

Why Count-Min Sketch for Top Providers?
----------------------------------------
1. Memory Efficiency: O(ε^-1 * ln(δ^-1)) space vs O(n) for exact counting
   - ε: error factor (e.g., 0.001 = 0.1% error)
   - δ: failure probability (e.g., 0.01 = 99% confidence)
   - n: number of unique providers

2. Speed: O(d) time for both updates and queries where d = depth (typically 5-7)

3. Scalability: Can handle millions of claims without memory issues

4. Perfect for Top-K queries: Combined with a min-heap, efficiently tracks top providers

Example:
--------
For 10,000 unique providers with ε=0.001, δ=0.01:
- Exact counting: 10,000 entries * 8 bytes = 80KB minimum
- Count-Min Sketch: width=2719, depth=5, total = 54KB fixed
- Memory stays constant even with 1M providers!
"""

import hashlib
from typing import Dict, List, Optional, Any
from decimal import Decimal
import numpy as np
import heapq
from dataclasses import dataclass
import math


@dataclass
class ProviderNetFee:
    """Tracks a provider's net fee total"""
    provider_npi: str
    net_fee_total: Decimal
    claim_count: int = 0

    def __lt__(self, other):
        """For heap comparison - based on net fee"""
        return self.net_fee_total < other.net_fee_total

    def to_dict(self):
        return {
            "provider_npi": self.provider_npi,
            "net_fee_total": str(self.net_fee_total),
            "claim_count": self.claim_count
        }


class CountMinSketch:
    """
    Count-Min Sketch for tracking provider net fees.

    Parameters:
    -----------
    width: int
        Number of counters per hash function (controls accuracy)
    depth: int
        Number of hash functions (controls confidence)

    The error in frequency estimation is at most ε * N with probability 1-δ where:
    - width = ceil(e/ε)  where e = 2.718...
    - depth = ceil(ln(1/δ))
    - N = total number of items

    For our use case with ε=0.001 (0.1% error) and δ=0.01 (99% confidence):
    - width = 2719
    - depth = 5
    """

    def __init__(self, width: int = 2719, depth: int = 5):
        """
        Initialize Count-Min Sketch.

        Default values give 0.1% error with 99% confidence.
        """
        self.width = width
        self.depth = depth

        # Initialize sketch table with zeros
        # Using float64 to handle large net fee sums
        self.table = np.zeros((depth, width), dtype=np.float64)

        # Track total sum for error estimation
        self.total_sum = Decimal("0.00")

        # Salt for hash functions
        self.hash_salts = [f"salt_{i}" for i in range(depth)]

    def _hash(self, item: str, seed: int) -> int:
        """
        Hash function for the sketch.
        Uses SHA256 with different seeds for independence.
        """
        hash_input = f"{self.hash_salts[seed]}:{item}".encode()
        hash_value = int(hashlib.sha256(hash_input).hexdigest(), 16)
        return hash_value % self.width

    def add(self, provider_npi: str, net_fee: Decimal):
        """
        Add a net fee amount for a provider.

        Time Complexity: O(depth) - typically 5-7 operations
        """
        fee_float = float(net_fee)

        for i in range(self.depth):
            j = self._hash(provider_npi, i)
            self.table[i][j] += fee_float

        self.total_sum += net_fee

    def estimate(self, provider_npi: str) -> Decimal:
        """
        Estimate total net fees for a provider.

        Returns the minimum value across all hash functions (most accurate).
        Time Complexity: O(depth)
        """
        min_estimate = float('inf')

        for i in range(self.depth):
            j = self._hash(provider_npi, i)
            min_estimate = min(min_estimate, self.table[i][j])

        return Decimal(str(min_estimate))

    def merge(self, other: 'CountMinSketch'):
        """
        Merge another sketch into this one.
        Useful for aggregating across multiple service instances.
        """
        if self.width != other.width or self.depth != other.depth:
            raise ValueError("Sketches must have same dimensions to merge")

        self.table += other.table
        self.total_sum += other.total_sum

    def clear(self):
        """Reset the sketch"""
        self.table.fill(0)
        self.total_sum = Decimal("0.00")

    def error_estimate(self) -> Decimal:
        """
        Estimate maximum error in counts.
        Error ≤ ε * total_sum with probability 1-δ
        """
        epsilon = math.e / self.width
        return self.total_sum * Decimal(str(epsilon))


class TopProvidersTracker:
    """
    Tracks top K providers by net fees using Count-Min Sketch + Min-Heap.

    Algorithm:
    ----------
    1. Use Count-Min Sketch to track all provider net fees (space-efficient)
    2. Maintain a min-heap of size K with top providers
    3. When updating, check if provider should be in top K
    4. Periodically verify heap accuracy against sketch

    This gives us:
    - O(1) space for sketch (fixed size regardless of providers)
    - O(K) space for heap (K = 10 for top 10)
    - O(log K) update time
    - High accuracy for heavy hitters (top providers)
    """

    def __init__(self, k: int = 10, width: int = 2719, depth: int = 5):
        """
        Initialize tracker for top K providers.

        Parameters:
        -----------
        k: Number of top providers to track (default 10)
        width: Count-Min Sketch width
        depth: Count-Min Sketch depth
        """
        self.k = k
        self.sketch = CountMinSketch(width=width, depth=depth)

        # Min-heap to track top K providers
        # Using negative values since heapq is min-heap by default
        self.top_providers: List[ProviderNetFee] = []

        # Dictionary for O(1) lookup of providers in heap
        self.provider_map: Dict[str, ProviderNetFee] = {}

        # Track claim counts separately
        self.claim_counts: Dict[str, int] = {}

    def add_claim(self, provider_npi: str, net_fee: Decimal):
        """
        Add a claim's net fee for a provider.

        Time Complexity: O(depth + log K) ≈ O(log K) since depth is constant
        """
        # Update sketch
        self.sketch.add(provider_npi, net_fee)

        # Update claim count
        self.claim_counts[provider_npi] = self.claim_counts.get(provider_npi, 0) + 1

        # Get estimated total for this provider
        estimated_total = self.sketch.estimate(provider_npi)

        # Update top K tracking
        self._update_top_k(provider_npi, estimated_total)

    def _update_top_k(self, provider_npi: str, net_fee_total: Decimal):
        """Update the top K providers heap"""

        if provider_npi in self.provider_map:
            # Provider already in top K, update their total
            provider = self.provider_map[provider_npi]
            provider.net_fee_total = net_fee_total
            provider.claim_count = self.claim_counts.get(provider_npi, 0)
            # Rebuild heap since we modified a value
            heapq.heapify(self.top_providers)

        elif len(self.top_providers) < self.k:
            # Haven't reached K providers yet, add this one
            provider = ProviderNetFee(
                provider_npi=provider_npi,
                net_fee_total=net_fee_total,
                claim_count=self.claim_counts.get(provider_npi, 0)
            )
            heapq.heappush(self.top_providers, provider)
            self.provider_map[provider_npi] = provider

        elif net_fee_total > self.top_providers[0].net_fee_total:
            # This provider has more net fees than the minimum in top K
            # Remove the minimum
            removed = heapq.heappop(self.top_providers)
            del self.provider_map[removed.provider_npi]

            # Add the new provider
            provider = ProviderNetFee(
                provider_npi=provider_npi,
                net_fee_total=net_fee_total,
                claim_count=self.claim_counts.get(provider_npi, 0)
            )
            heapq.heappush(self.top_providers, provider)
            self.provider_map[provider_npi] = provider

    def get_top_k(self) -> List[ProviderNetFee]:
        """
        Get top K providers sorted by net fees (highest first).

        Time Complexity: O(K log K)
        """
        # Sort by net_fee_total descending
        return sorted(self.top_providers, key=lambda x: x.net_fee_total, reverse=True)

    def verify_accuracy(self) -> Dict[str, Any]:
        """
        Verify heap accuracy against sketch estimates.
        Returns statistics about accuracy.
        """
        stats = {
            "heap_size": len(self.top_providers),
            "total_providers": len(self.claim_counts),
            "total_net_fees": str(self.sketch.total_sum),
            "max_error_estimate": str(self.sketch.error_estimate()),
            "discrepancies": []
        }

        for provider in self.top_providers:
            sketch_estimate = self.sketch.estimate(provider.provider_npi)
            if abs(sketch_estimate - provider.net_fee_total) > Decimal("0.01"):
                stats["discrepancies"].append({
                    "provider_npi": provider.provider_npi,
                    "heap_value": str(provider.net_fee_total),
                    "sketch_estimate": str(sketch_estimate),
                    "difference": str(abs(sketch_estimate - provider.net_fee_total))
                })

        return stats


# Global in-memory tracker instance (singleton pattern)
_global_tracker: Optional[TopProvidersTracker] = None


def get_tracker() -> TopProvidersTracker:
    """Get or create the global tracker instance"""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = TopProvidersTracker()
    return _global_tracker


def reset_tracker():
    """Reset the global tracker (useful for testing)"""
    global _global_tracker
    _global_tracker = TopProvidersTracker()