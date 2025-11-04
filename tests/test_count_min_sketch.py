"""Tests for Count-Min Sketch implementation."""
import pytest
from decimal import Decimal
from claim_process.count_min_sketch import CountMinSketch, TopProvidersTracker


class TestCountMinSketch:
    """Test Count-Min Sketch functionality."""

    def test_sketch_initialization(self):
        """Test sketch initializes with correct dimensions."""
        sketch = CountMinSketch(width=100, depth=5)
        assert sketch.width == 100
        assert sketch.depth == 5
        assert sketch.table.shape == (5, 100)
        assert sketch.total_sum == Decimal("0.00")

    def test_add_and_estimate(self):
        """Test adding values and getting estimates."""
        sketch = CountMinSketch(width=1000, depth=5)

        # Add some values
        sketch.add("provider1", Decimal("100.00"))
        sketch.add("provider1", Decimal("50.00"))
        sketch.add("provider2", Decimal("200.00"))

        # Estimates should be close to actual values
        estimate1 = sketch.estimate("provider1")
        estimate2 = sketch.estimate("provider2")

        assert estimate1 >= Decimal("150.00")  # Should be at least the actual value
        assert estimate2 >= Decimal("200.00")
        assert sketch.total_sum == Decimal("350.00")

    def test_error_bounds(self):
        """Test that error is within expected bounds."""
        sketch = CountMinSketch(width=2719, depth=5)  # 0.1% error, 99% confidence

        # Add many values
        for i in range(100):
            sketch.add(f"provider_{i}", Decimal("100.00"))

        # Check error estimate
        error = sketch.error_estimate()
        # Error should be approximately 0.1% of total
        assert error <= sketch.total_sum * Decimal("0.002")  # Allow some margin


class TestTopProvidersTracker:
    """Test top providers tracking functionality."""

    def test_tracker_initialization(self):
        """Test tracker initializes correctly."""
        tracker = TopProvidersTracker(k=10)
        assert tracker.k == 10
        assert len(tracker.top_providers) == 0
        assert len(tracker.provider_map) == 0

    def test_add_claims_and_get_top_k(self):
        """Test adding claims and retrieving top providers."""
        tracker = TopProvidersTracker(k=3)

        # Add claims for different providers
        tracker.add_claim("npi1", Decimal("100.00"))
        tracker.add_claim("npi2", Decimal("200.00"))
        tracker.add_claim("npi3", Decimal("150.00"))
        tracker.add_claim("npi1", Decimal("50.00"))  # Additional claim for npi1
        tracker.add_claim("npi4", Decimal("50.00"))   # Won't be in top 3

        top_providers = tracker.get_top_k()

        # Check top 3 providers
        assert len(top_providers) == 3
        assert top_providers[0].provider_npi == "npi2"
        assert top_providers[0].net_fee_total == Decimal("200.00")
        assert top_providers[1].provider_npi == "npi3"
        assert top_providers[1].net_fee_total == Decimal("150.00")
        assert top_providers[2].provider_npi == "npi1"
        assert top_providers[2].net_fee_total >= Decimal("150.00")  # At least 150

    def test_claim_count_tracking(self):
        """Test that claim counts are tracked correctly."""
        tracker = TopProvidersTracker(k=2)

        tracker.add_claim("npi1", Decimal("100.00"))
        tracker.add_claim("npi1", Decimal("50.00"))
        tracker.add_claim("npi1", Decimal("25.00"))

        assert tracker.claim_counts["npi1"] == 3

        top_providers = tracker.get_top_k()
        assert top_providers[0].claim_count == 3

    def test_heap_updates(self):
        """Test that heap updates correctly when new top provider emerges."""
        tracker = TopProvidersTracker(k=2)

        # Fill up the top 2
        tracker.add_claim("npi1", Decimal("100.00"))
        tracker.add_claim("npi2", Decimal("150.00"))

        # Add a new provider that should replace npi1
        tracker.add_claim("npi3", Decimal("200.00"))

        top_providers = tracker.get_top_k()
        provider_npis = [p.provider_npi for p in top_providers]

        assert "npi3" in provider_npis
        assert "npi2" in provider_npis
        assert "npi1" not in provider_npis  # Should be evicted

    def test_verify_accuracy(self):
        """Test accuracy verification method."""
        tracker = TopProvidersTracker(k=3)

        tracker.add_claim("npi1", Decimal("100.00"))
        tracker.add_claim("npi2", Decimal("200.00"))

        stats = tracker.verify_accuracy()

        assert stats["heap_size"] == 2
        assert stats["total_providers"] == 2
        assert stats["total_net_fees"] == "300.00"
        assert len(stats["discrepancies"]) == 0  # Should have no major discrepancies