"""Integration tests for API endpoints with clean API format."""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, create_engine, SQLModel
from sqlalchemy.pool import StaticPool

from claim_process.main import app, get_session
from claim_process.models import Claim
from claim_process.count_min_sketch import reset_tracker


# Create in-memory database for testing
@pytest.fixture(name="session")
def session_fixture():
    """Create a new database session for testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(name="client")
def client_fixture(session: Session):
    """Create test client with overridden dependencies."""
    # Reset Count-Min Sketch tracker for clean test state
    reset_tracker()

    def get_session_override():
        return session

    app.dependency_overrides[get_session] = get_session_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


class TestHealthEndpoint:
    """Test health check endpoint."""

    def test_health_check(self, client):
        """Test that health endpoint returns correct status."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "claim_process"}


class TestClaimProcessing:
    """Test claim processing endpoint."""

    def test_process_valid_claim(self, client):
        """Test processing a valid claim with clean API format."""
        claim_data = {
            "external_claim_id": "test_123",
            "lines": [
                {
                    "service_date": "2018-03-28",
                    "submitted_procedure": "D0180",
                    "quadrant": None,
                    "plan_group_number": "GRP-1000",
                    "subscriber_number": "3730189502",
                    "provider_npi": "1497775530",
                    "provider_fees": "100.00",
                    "allowed_fees": "100.00",
                    "member_coinsurance": "0.00",
                    "member_copay": "0.00"
                },
                {
                    "service_date": "2018-03-28",
                    "submitted_procedure": "D4346",
                    "quadrant": None,
                    "plan_group_number": "GRP-1000",
                    "subscriber_number": "3730189502",
                    "provider_npi": "1497775530",
                    "provider_fees": "130.00",
                    "allowed_fees": "65.00",
                    "member_coinsurance": "16.25",
                    "member_copay": "0.00"
                }
            ]
        }

        response = client.post("/claims", json=claim_data)
        assert response.status_code == 200

        result = response.json()
        assert result["external_claim_id"] == "test_123"
        assert result["provider_npi"] == "1497775530"
        assert result["line_count"] == 2
        # Net fee = (100 + 0 + 0 - 100) + (130 + 16.25 + 0 - 65) = 0 + 81.25 = 81.25
        assert result["net_fee"] == "81.25"

    def test_process_claim_with_messy_fields(self, client):
        """Test processing claim where line keys use inconsistent casing."""
        claim_data = {
            "external_claim_id": "messy_123",
            "lines": [
                {
                    "Service Date": "3/28/18 0:00",
                    "submitted procedure": "d0180",
                    "Plan/Group #": "GRP-1000",
                    "Subscriber#": "3730189502",
                    "Provider NPI": "1497775530",
                    "provider fees": "$100.00 ",
                    "Allowed fees": "$100.00",
                    "member coinsurance": "$0.00",
                    "member copay": "$0.00",
                },
                {
                    "service date": "2018-03-28",
                    "Submitted Procedure": "D4346",
                    "Quadrant": None,
                    "plan/group #": "GRP-1000",
                    "subscriber#": "3730189502",
                    "provider npi": "1497775530",
                    "PROVIDER FEES": "$130.00",
                    "allowed_fees": "$65.00",
                    "Member Coinsurance": "16.25",
                    "Member Copay": "0.00",
                },
            ]
        }

        response = client.post("/claims", json=claim_data)
        assert response.status_code == 200

        result = response.json()
        assert result["external_claim_id"] == "messy_123"
        assert result["provider_npi"] == "1497775530"
        assert result["line_count"] == 2
        # Net fee = (100 + 0 + 0 - 100) + (130 + 16.25 + 0 - 65) = 81.25
        assert result["net_fee"] == "81.25"

    def test_process_claim_missing_required_field(self, client):
        """Test processing claim with missing required field."""
        claim_data = {
            "lines": [
                {
                    "service_date": "2018-03-28",
                    "submitted_procedure": "D0180",
                    # Missing provider_npi
                    "quadrant": None,
                    "plan_group_number": "GRP-1000",
                    "subscriber_number": "3730189502",
                    "provider_fees": "100.00",
                    "allowed_fees": "100.00",
                    "member_coinsurance": "0.00",
                    "member_copay": "0.00"
                }
            ]
        }

        response = client.post("/claims", json=claim_data)
        assert response.status_code == 422  # Validation error

    def test_process_claim_invalid_npi(self, client):
        """Test processing claim with invalid NPI."""
        claim_data = {
            "lines": [
                {
                    "service_date": "2018-03-28",
                    "submitted_procedure": "D0180",
                    "quadrant": None,
                    "plan_group_number": "GRP-1000",
                    "subscriber_number": "3730189502",
                    "provider_npi": "12345",  # Invalid - not 10 digits
                    "provider_fees": "100.00",
                    "allowed_fees": "100.00",
                    "member_coinsurance": "0.00",
                    "member_copay": "0.00"
                }
            ]
        }

        response = client.post("/claims", json=claim_data)
        assert response.status_code == 400  # Business validation error

    def test_process_claim_invalid_procedure_code(self, client):
        """Test processing claim with invalid procedure code."""
        claim_data = {
            "lines": [
                {
                    "service_date": "2018-03-28",
                    "submitted_procedure": "X0180",  # Invalid - doesn't start with D
                    "quadrant": None,
                    "plan_group_number": "GRP-1000",
                    "subscriber_number": "3730189502",
                    "provider_npi": "1497775530",
                    "provider_fees": "100.00",
                    "allowed_fees": "100.00",
                    "member_coinsurance": "0.00",
                    "member_copay": "0.00"
                }
            ]
        }

        response = client.post("/claims", json=claim_data)
        assert response.status_code == 400  # Business validation error


class TestTopProvidersEndpoint:
    """Test top providers endpoint."""

    def test_get_top_providers_empty(self, client):
        """Test getting top providers with no claims."""
        response = client.get("/top-providers")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_top_providers_with_data(self, client, session):
        """Test getting top providers after processing claims."""
        # Process multiple claims
        claims_data = [
            {
                "lines": [{
                    "service_date": "2018-03-28",
                    "submitted_procedure": "D0180",
                    "quadrant": None,
                    "plan_group_number": "GRP-1000",
                    "subscriber_number": f"373018950{i}",
                    "provider_npi": f"149777553{i % 3}",  # 3 different providers
                    "provider_fees": f"{100 * (i + 1)}.00",
                    "allowed_fees": "80.00",
                    "member_coinsurance": "10.00",
                    "member_copay": "5.00"
                }]
            }
            for i in range(5)
        ]

        # Process claims
        for claim_data in claims_data:
            response = client.post("/claims", json=claim_data)
            assert response.status_code == 200

        # Get top providers
        response = client.get("/top-providers?limit=3")
        assert response.status_code == 200

        top_providers = response.json()
        assert len(top_providers) <= 3
        # Verify they're sorted by rank
        for i in range(len(top_providers) - 1):
            assert top_providers[i]["rank"] < top_providers[i + 1]["rank"]


class TestGetClaimEndpoint:
    """Test get claim by ID endpoint."""

    def test_get_existing_claim(self, client):
        """Test getting an existing claim."""
        # First create a claim
        claim_data = {
            "external_claim_id": "test_get_123",
            "lines": [{
                "service_date": "2018-03-28",
                "submitted_procedure": "D0180",
                "quadrant": None,
                "plan_group_number": "GRP-1000",
                "subscriber_number": "3730189502",
                "provider_npi": "1497775530",
                "provider_fees": "100.00",
                "allowed_fees": "100.00",
                "member_coinsurance": "0.00",
                "member_copay": "0.00"
            }]
        }

        create_response = client.post("/claims", json=claim_data)
        assert create_response.status_code == 200
        created_claim = create_response.json()

        # Now get the claim
        get_response = client.get(f"/claims/{created_claim['claim_id']}")
        assert get_response.status_code == 200

        retrieved_claim = get_response.json()
        assert retrieved_claim["claim_id"] == created_claim["claim_id"]
        assert retrieved_claim["external_claim_id"] == "test_get_123"

    def test_get_nonexistent_claim(self, client):
        """Test getting a non-existent claim."""
        response = client.get("/claims/00000000-0000-0000-0000-000000000000")
        assert response.status_code == 404
        assert response.json()["detail"] == "Claim not found"