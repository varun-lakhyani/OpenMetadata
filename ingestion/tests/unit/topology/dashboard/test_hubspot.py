#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Unit tests for HubSpot dashboard connector
"""
from unittest.mock import MagicMock, patch

import pytest
from requests import HTTPError

from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import (
    CreateDashboardDataModelRequest,
)
from metadata.generated.schema.entity.data.chart import ChartType
from metadata.generated.schema.entity.data.dashboardDataModel import DataModelType
from metadata.generated.schema.entity.data.table import DataType
from metadata.generated.schema.entity.services.connections.dashboard.hubspotConnection import (
    HubspotConnection,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.source.dashboard.hubspot.client import (
    STANDARD_CRM_OBJECT_TYPES,
    STANDARD_PIPELINE_OBJECT_TYPES,
    HubSpotClient,
)
from metadata.ingestion.source.dashboard.hubspot.metadata import (
    HUBSPOT_PROPERTY_TYPE_MAP,
    HubspotSource,
)
from metadata.ingestion.source.dashboard.hubspot.models import (
    HubSpotCRMObjectSchema,
    HubSpotCRMProperty,
    HubSpotEventDefinition,
    HubSpotEventProperty,
    HubSpotPipeline,
    HubSpotPipelineStage,
)

MOCK_CONFIG = {
    "source": {
        "type": "Hubspot",
        "serviceName": "test_hubspot",
        "serviceConnection": {
            "config": {
                "type": "Hubspot",
                "accessToken": "test-token",
                "hubId": "12345",
            }
        },
        "sourceConfig": {"config": {"type": "DashboardMetadata"}},
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "openMetadataServerConfig": {
            "hostPort": "http://localhost:8585/api",
            "authProvider": "openmetadata",
            "securityConfig": {"jwtToken": "test-token"},
        }
    },
}

MOCK_STAGES = [
    HubSpotPipelineStage(
        id="s1", label="Prospecting", displayOrder=0, probability="0.1"
    ),
    HubSpotPipelineStage(
        id="s2", label="Closed Won", displayOrder=1, probability="1.0", isClosed=True
    ),
]

MOCK_PIPELINE = HubSpotPipeline(
    id="pipeline-1",
    label="Sales Pipeline",
    displayOrder=0,
    archived=False,
    stages=MOCK_STAGES,
    objectType="deals",
)

MOCK_EVENT_PROPS = [
    HubSpotEventProperty(
        name="email", label="Email", type="string", description="Contact email"
    ),
    HubSpotEventProperty(name="revenue", label="Revenue", type="number"),
    HubSpotEventProperty(name="plan", label="Plan", type="enumeration"),
]

MOCK_EVENT_DEF = HubSpotEventDefinition(
    name="pe123456_form_submitted",
    label="Form Submitted",
    description="A form was submitted",
    properties=MOCK_EVENT_PROPS,
)

MOCK_CRM_SCHEMA = HubSpotCRMObjectSchema(
    name="contacts",
    label="Contacts",
    properties=[
        HubSpotCRMProperty(name="email", label="Email Address", type="string"),
        HubSpotCRMProperty(name="revenue", label="Annual Revenue", type="number"),
    ],
)


class TestHubSpotClient:
    @pytest.fixture
    def client(self):
        connection = HubspotConnection(accessToken="test-secret-token")
        with patch("metadata.ingestion.source.dashboard.hubspot.client.HubSpot"):
            with patch(
                "metadata.ingestion.source.dashboard.hubspot.client.requests.Session"
            ):
                c = HubSpotClient(connection)
                yield c

    def test_close_closes_session(self, client):
        client.close()
        client._session.close.assert_called_once()

    def test_get_pipelines_returns_all_standard_types(self, client):
        client._custom_object_type_infos = []
        mock_stage = MagicMock()
        mock_stage.id = "s1"
        mock_stage.label = "Stage 1"
        mock_stage.display_order = 0
        mock_stage.metadata = {"probability": "0.75", "isClosed": "false"}

        mock_pipeline = MagicMock()
        mock_pipeline.id = "pl1"
        mock_pipeline.label = "Pipeline"
        mock_pipeline.display_order = 0
        mock_pipeline.archived = False
        mock_pipeline.stages = [mock_stage]

        client._api.crm.pipelines.pipelines_api.get_all.return_value = MagicMock(
            results=[mock_pipeline]
        )

        pipelines = client.get_pipelines()

        assert len(pipelines) == len(STANDARD_PIPELINE_OBJECT_TYPES)
        assert [p.object_type for p in pipelines] == STANDARD_PIPELINE_OBJECT_TYPES
        assert pipelines[0].stages[0].probability == "0.75"

    def test_get_pipelines_skips_failed_object_type(self, client):
        client._custom_object_type_infos = []

        def side_effect(object_type):
            if object_type == "deals":
                raise RuntimeError("deals unavailable")
            mock_pipeline = MagicMock()
            mock_pipeline.id = "pl1"
            mock_pipeline.label = "Ticket Pipeline"
            mock_pipeline.display_order = 0
            mock_pipeline.archived = False
            mock_pipeline.stages = []
            return MagicMock(results=[mock_pipeline])

        client._api.crm.pipelines.pipelines_api.get_all.side_effect = side_effect

        pipelines = client.get_pipelines()
        assert len(pipelines) == len(STANDARD_PIPELINE_OBJECT_TYPES) - 1
        assert "deals" not in {p.object_type for p in pipelines}

    def test_get_event_definitions_paginates(self, client):
        page1 = {
            "results": [
                {
                    "name": "pe_event_1",
                    "label": "Event 1",
                    "propertyDefinitions": [
                        {"name": "prop1", "label": "Prop 1", "type": "string"}
                    ],
                }
            ],
            "paging": {"next": {"after": "cursor-abc"}},
        }
        page2 = {
            "results": [
                {"name": "pe_event_2", "label": "Event 2", "propertyDefinitions": []}
            ],
        }
        client._session = MagicMock()
        client._session.get.return_value.status_code = 200
        client._session.get.return_value.raise_for_status = MagicMock()
        client._session.get.return_value.json.side_effect = [page1, page2]

        events = client.get_event_definitions()

        assert len(events) == 2
        assert events[0].name == "pe_event_1"
        assert events[1].name == "pe_event_2"
        assert events[0].properties[0].name == "prop1"

    def test_get_event_definitions_any_exception_returns_empty(self, client):
        client._session = MagicMock()
        client._session.get.side_effect = RuntimeError("network error")

        assert client.get_event_definitions() == []

    def test_get_event_definitions_scope_error_returns_empty(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 403
        scope_error = HTTPError(response=mock_response)

        client._session = MagicMock()
        client._session.get.return_value.raise_for_status.side_effect = scope_error

        assert client.get_event_definitions() == []

    def test_get_crm_object_schemas_covers_all_standard_types(self, client):
        client._session = MagicMock()
        client._session.get.return_value.raise_for_status = MagicMock()
        client._session.get.return_value.json.return_value = {
            "results": [
                {
                    "name": "prop1",
                    "label": "Prop 1",
                    "type": "string",
                    "archived": False,
                    "hidden": False,
                }
            ]
        }
        client._custom_object_type_infos = []

        schemas = client.get_crm_object_schemas()

        assert len(schemas) == len(STANDARD_CRM_OBJECT_TYPES)
        assert {s.name for s in schemas} == set(STANDARD_CRM_OBJECT_TYPES)

    def test_get_crm_object_schemas_line_items_label(self, client):
        client._session = MagicMock()
        client._session.get.return_value.raise_for_status = MagicMock()
        client._session.get.return_value.json.return_value = {"results": []}
        client._custom_object_type_infos = []

        schemas = client.get_crm_object_schemas()

        line_items_schema = next(s for s in schemas if s.name == "line_items")
        assert line_items_schema.label == "Line Items"

    def test_get_crm_object_schemas_filters_archived_and_hidden(self, client):
        client._session = MagicMock()
        client._session.get.return_value.raise_for_status = MagicMock()
        client._session.get.return_value.json.return_value = {
            "results": [
                {
                    "name": "visible",
                    "label": "Visible",
                    "type": "string",
                    "archived": False,
                    "hidden": False,
                },
                {
                    "name": "archived_prop",
                    "type": "string",
                    "archived": True,
                    "hidden": False,
                },
                {
                    "name": "hidden_prop",
                    "type": "string",
                    "archived": False,
                    "hidden": True,
                },
            ]
        }
        client._custom_object_type_infos = []

        schemas = client.get_crm_object_schemas()

        contacts_schema = next(s for s in schemas if s.name == "contacts")
        prop_names = {p.name for p in contacts_schema.properties}
        assert "visible" in prop_names
        assert "archived_prop" not in prop_names
        assert "hidden_prop" not in prop_names

    def test_get_crm_object_schemas_paginates(self, client):
        page1 = {
            "results": [
                {
                    "name": "p1",
                    "label": "P1",
                    "type": "string",
                    "archived": False,
                    "hidden": False,
                }
            ],
            "paging": {"next": {"after": "cursor-1"}},
        }
        page2 = {
            "results": [
                {
                    "name": "p2",
                    "label": "P2",
                    "type": "number",
                    "archived": False,
                    "hidden": False,
                }
            ]
        }
        client._session = MagicMock()
        client._session.get.return_value.status_code = 200
        client._session.get.return_value.raise_for_status = MagicMock()
        client._session.get.return_value.json.side_effect = [page1, page2] * len(
            STANDARD_CRM_OBJECT_TYPES
        )
        client._custom_object_type_infos = []

        schemas = client.get_crm_object_schemas()

        assert len(schemas) == len(STANDARD_CRM_OBJECT_TYPES)
        for schema in schemas:
            assert (
                len(schema.properties) == 2
            ), f"{schema.name} had {len(schema.properties)} properties"

    def test_get_crm_object_schemas_scope_error_skips_type(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 403
        scope_error = HTTPError(response=mock_response)

        client._session = MagicMock()
        client._session.get.return_value.raise_for_status.side_effect = scope_error
        client._custom_object_type_infos = []

        assert client.get_crm_object_schemas() == []

    def test_is_scope_error_detects_http_403(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 403
        assert client._is_scope_error(HTTPError(response=mock_response)) is True

        mock_response.status_code = 500
        assert client._is_scope_error(HTTPError(response=mock_response)) is False

    def test_is_scope_error_detects_sdk_403(self, client):
        exc = Exception("SDK error")
        exc.status = 403
        assert client._is_scope_error(exc) is True
        assert client._is_scope_error(RuntimeError("network error")) is False

    def test_custom_object_type_infos_cached(self, client):
        client._custom_object_type_infos = []
        client._session = MagicMock()

        result1 = client._get_custom_object_type_infos()
        result2 = client._get_custom_object_type_infos()

        assert result1 is result2
        client._session.get.assert_not_called()

    def test_custom_object_type_infos_paginates(self, client):
        page1 = {
            "results": [
                {"name": "custom_obj_1", "objectTypeId": "1", "hasPipeline": True}
            ],
            "paging": {"next": {"after": "cursor-abc"}},
        }
        page2 = {
            "results": [
                {"name": "custom_obj_2", "objectTypeId": "2", "hasPipeline": False}
            ]
        }
        client._session = MagicMock()
        client._session.get.return_value.raise_for_status = MagicMock()
        client._session.get.return_value.json.side_effect = [page1, page2]

        infos = client._get_custom_object_type_infos()

        assert len(infos) == 2
        assert infos[0].name == "custom_obj_1"
        assert infos[0].has_pipeline is True
        assert infos[1].has_pipeline is False

    def test_get_retries_on_429_then_succeeds(self, client):
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "1"}

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.raise_for_status = MagicMock()
        success_response.json.return_value = {"results": []}

        client._session = MagicMock()
        client._session.get.side_effect = [rate_limit_response, success_response]

        with patch(
            "metadata.ingestion.source.dashboard.hubspot.client.time.sleep"
        ) as mock_sleep:
            result = client._get("/test/path")

        assert result == {"results": []}
        mock_sleep.assert_called_once_with(1)

    def test_get_raises_after_3_rate_limit_retries(self, client):
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "1"}

        client._session = MagicMock()
        client._session.get.return_value = rate_limit_response

        with patch("metadata.ingestion.source.dashboard.hubspot.client.time.sleep"):
            with pytest.raises(
                RuntimeError, match="rate limit exceeded after 3 retries"
            ):
                client._get("/test/path")

    def test_get_uses_default_retry_after_on_invalid_header(self, client):
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "not-a-number"}

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.raise_for_status = MagicMock()
        success_response.json.return_value = {}

        client._session = MagicMock()
        client._session.get.side_effect = [rate_limit_response, success_response]

        with patch(
            "metadata.ingestion.source.dashboard.hubspot.client.time.sleep"
        ) as mock_sleep:
            client._get("/test/path")

        mock_sleep.assert_called_once_with(10)


class TestPropertyTypeMap:
    def test_all_hubspot_types_mapped(self):
        expected_types = {
            "string",
            "number",
            "enumeration",
            "datetime",
            "date",
            "bool",
            "json",
            "phone_number",
            "currency",
            "html",
        }
        assert set(HUBSPOT_PROPERTY_TYPE_MAP.keys()) == expected_types

    def test_known_type_mappings(self):
        assert HUBSPOT_PROPERTY_TYPE_MAP["string"] == DataType.VARCHAR
        assert HUBSPOT_PROPERTY_TYPE_MAP["number"] == DataType.DOUBLE
        assert HUBSPOT_PROPERTY_TYPE_MAP["enumeration"] == DataType.ENUM
        assert HUBSPOT_PROPERTY_TYPE_MAP["bool"] == DataType.BOOLEAN


@pytest.fixture
def mock_source():
    """Return a HubspotSource with all external connections mocked."""
    with patch("metadata.ingestion.source.dashboard.hubspot.connection.get_connection"):
        with patch(
            "metadata.ingestion.source.dashboard.dashboard_service.OpenMetadata"
        ):
            from metadata.generated.schema.metadataIngestion.workflow import (
                OpenMetadataWorkflowConfig,
            )

            config = OpenMetadataWorkflowConfig.model_validate(MOCK_CONFIG)
            source = HubspotSource.__new__(HubspotSource)
            source.config = config.source
            source.metadata = MagicMock()
            source.client = MagicMock()
            source.service_connection = config.source.serviceConnection.root.config
            source.source_config = MagicMock()
            source.source_config.chartFilterPattern = None
            source.status = MagicMock()

            ctx = MagicMock()
            ctx.dashboard_service = "test_hubspot"
            ctx.charts = [f"pipeline-1-s{i}" for i in range(2)]
            source.context = MagicMock()
            source.context.get.return_value = ctx

            yield source


class TestHubspotSource:
    def test_create_raises_for_wrong_connection_type(self):
        mock_workflow_source = MagicMock()
        mock_workflow_source.serviceConnection.root.config = MagicMock()

        with patch(
            "metadata.ingestion.source.dashboard.hubspot.metadata.WorkflowSource.model_validate",
            return_value=mock_workflow_source,
        ):
            with pytest.raises(InvalidSourceException):
                HubspotSource.create(config_dict=MOCK_CONFIG, metadata=MagicMock())

    def test_list_datamodels_yields_crm_schemas_and_events(self, mock_source):
        mock_source.client.get_crm_object_schemas.return_value = [MOCK_CRM_SCHEMA]
        mock_source.client.get_event_definitions.return_value = [MOCK_EVENT_DEF]

        results = list(mock_source.list_datamodels())

        assert len(results) == 2
        assert results[0] is MOCK_CRM_SCHEMA
        assert results[1] is MOCK_EVENT_DEF

    def test_yield_bulk_datamodel_event_definition(self, mock_source):
        results = list(mock_source.yield_bulk_datamodel(MOCK_EVENT_DEF))

        assert len(results) == 1
        req: CreateDashboardDataModelRequest = results[0].right
        assert str(req.name.root) == "pe123456_form_submitted"
        assert req.displayName == "Form Submitted"
        assert req.dataModelType == DataModelType.HubspotDataModel
        assert len(req.columns) == 3

        col_types = {c.name.root: c.dataType for c in req.columns}
        assert col_types["email"] == DataType.VARCHAR
        assert col_types["revenue"] == DataType.DOUBLE
        assert col_types["plan"] == DataType.ENUM

    def test_yield_bulk_datamodel_crm_schema(self, mock_source):
        results = list(mock_source.yield_bulk_datamodel(MOCK_CRM_SCHEMA))

        assert len(results) == 1
        req: CreateDashboardDataModelRequest = results[0].right
        assert str(req.name.root) == "contacts"
        assert req.displayName == "Contacts"
        assert req.description is None
        assert req.dataModelType == DataModelType.HubspotDataModel
        assert len(req.columns) == 2

        col_types = {c.name.root: c.dataType for c in req.columns}
        assert col_types["email"] == DataType.VARCHAR
        assert col_types["revenue"] == DataType.DOUBLE

    def test_yield_bulk_datamodel_unknown_type_defaults_to_varchar(self, mock_source):
        event_def = HubSpotEventDefinition(
            name="pe_test",
            properties=[HubSpotEventProperty(name="x", label="X", type="unknown_type")],
        )
        results = list(mock_source.yield_bulk_datamodel(event_def))
        assert results[0].right.columns[0].dataType == DataType.VARCHAR

    def test_yield_bulk_datamodel_exception_yields_error(self, mock_source):
        broken_def = MagicMock()
        broken_def.name = "broken"
        broken_def.properties = None
        results = list(mock_source.yield_bulk_datamodel(broken_def))
        assert results[0].left is not None
        assert "broken" in results[0].left.name

    def test_yield_dashboard_deals_url_contains_hub_id(self, mock_source):
        results = list(mock_source.yield_dashboard(MOCK_PIPELINE))

        req: CreateDashboardRequest = results[0].right
        assert str(req.name.root) == "pipeline-1"
        assert req.displayName == "Sales Pipeline"
        assert "deals" in str(req.sourceUrl.root)
        assert "12345" in str(req.sourceUrl.root)

    def test_yield_dashboard_tickets_url(self, mock_source):
        tickets_pipeline = HubSpotPipeline(
            id="tp-1", label="Support", objectType="tickets", stages=[]
        )
        results = list(mock_source.yield_dashboard(tickets_pipeline))
        assert "tickets" in str(results[0].right.sourceUrl.root)

    def test_yield_dashboard_no_source_url_when_hub_id_missing(self, mock_source):
        mock_source.service_connection = MagicMock()
        mock_source.service_connection.hubId = None
        results = list(mock_source.yield_dashboard(MOCK_PIPELINE))
        assert results[0].right.sourceUrl is None

    def test_yield_dashboard_custom_object_has_no_source_url(self, mock_source):
        custom_pipeline = HubSpotPipeline(
            id="cust-1", label="Custom", objectType="my_custom_object", stages=[]
        )
        results = list(mock_source.yield_dashboard(custom_pipeline))
        assert results[0].right.sourceUrl is None

    def test_yield_dashboard_exception_yields_error(self, mock_source):
        broken = MagicMock()
        broken.id = None
        broken.label = "broken"
        broken.object_type = "deals"
        broken.stages = []
        mock_source.context.get.return_value.charts = None
        results = list(mock_source.yield_dashboard(broken))
        assert results[0].left is not None
        assert "broken" in results[0].left.name

    def test_yield_dashboard_chart_yields_one_per_stage(self, mock_source):
        results = list(mock_source.yield_dashboard_chart(MOCK_PIPELINE))

        assert len(results) == 2
        for r in results:
            assert r.right is not None
            assert r.right.chartType == ChartType.Other

    def test_yield_dashboard_chart_probability_in_description(self, mock_source):
        results = list(mock_source.yield_dashboard_chart(MOCK_PIPELINE))
        prospecting = next(
            r for r in results if "Prospecting" in str(r.right.displayName)
        )
        assert "10%" in str(prospecting.right.description.root)

    def test_yield_dashboard_chart_no_source_url_when_hub_id_missing(self, mock_source):
        mock_source.service_connection = MagicMock()
        mock_source.service_connection.hubId = None
        results = list(mock_source.yield_dashboard_chart(MOCK_PIPELINE))
        for r in results:
            assert r.right.sourceUrl is None

    def test_yield_dashboard_chart_custom_object_has_no_source_url(self, mock_source):
        custom_pipeline = HubSpotPipeline(
            id="cust-1",
            label="Custom",
            objectType="my_custom_object",
            stages=[HubSpotPipelineStage(id="s1", label="Stage 1")],
        )
        results = list(mock_source.yield_dashboard_chart(custom_pipeline))
        assert results[0].right.sourceUrl is None

    def test_yield_dashboard_chart_filter_skips_stage(self, mock_source):
        with patch(
            "metadata.ingestion.source.dashboard.hubspot.metadata.filter_by_chart",
            side_effect=lambda pattern, name: name == "Prospecting",
        ):
            results = list(mock_source.yield_dashboard_chart(MOCK_PIPELINE))

        assert len(results) == 1
        displayed = {r.right.displayName for r in results}
        assert "Prospecting" not in displayed
        assert "Closed Won" in displayed

    def test_yield_dashboard_chart_exception_yields_error(self, mock_source):
        broken_stage = HubSpotPipelineStage(
            id="s1", label="broken-stage", probability="not-a-float"
        )
        broken_pipeline = HubSpotPipeline(
            id="p1", label="P", objectType="deals", stages=[broken_stage]
        )
        results = list(mock_source.yield_dashboard_chart(broken_pipeline))
        assert results[0].left is not None

    def test_yield_dashboard_lineage_yields_nothing(self, mock_source):
        assert list(mock_source.yield_dashboard_lineage_details(MOCK_PIPELINE)) == []
