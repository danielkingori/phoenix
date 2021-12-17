"""Top level conftest."""
import mock
import pytest

from phoenix.common.config import tenant


@pytest.fixture
def tenants_template_url_mock():
    """Fixture for mocking tenant functionality to use the tenant template."""
    test_url = f"file:///{tenant.TENANTS_TEMPLATE_PATH}"
    with mock.patch(
        "phoenix.common.config.tenant.get_config_url", return_value=test_url
    ) as mock_object:
        yield mock_object
