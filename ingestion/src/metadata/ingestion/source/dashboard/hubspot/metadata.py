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
HubSpot source module.

Entity mapping:
  - CRM Pipeline (deals, tickets, orders + custom pipeline-enabled objects) → Dashboard
  - Pipeline Stage                                                           → Chart
  - CRM Object Schema (contacts, companies, deals, tickets, orders,
    products, line_items, quotes + custom objects)                          → DashboardDataModel
  - Custom Behavioral Event Definition                                       → DashboardDataModel
      (requires behavioral_events.event_definitions.read_write scope)
"""
import traceback
from typing import Iterable, List, Optional, Union

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import (
    CreateDashboardDataModelRequest,
)
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.chart import Chart, ChartType
from metadata.generated.schema.entity.data.dashboardDataModel import DataModelType
from metadata.generated.schema.entity.data.table import Column, DataType
from metadata.generated.schema.entity.services.connections.dashboard.hubspotConnection import (
    HubspotConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
    SourceUrl,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.dashboard.dashboard_service import DashboardServiceSource
from metadata.ingestion.source.dashboard.hubspot.models import (
    HubSpotCRMObjectSchema,
    HubSpotEventDefinition,
    HubSpotPipeline,
)
from metadata.utils import fqn
from metadata.utils.filters import filter_by_chart
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

PIPELINE_SOURCE_URLS = {
    "deals": "https://app.hubspot.com/contacts/{hub_id}/deals/list/view/all/",
    "tickets": "https://app.hubspot.com/contacts/{hub_id}/tickets/list/view/all/",
    "orders": "https://app.hubspot.com/contacts/{hub_id}/orders/",
}

HUBSPOT_PROPERTY_TYPE_MAP = {
    "string": DataType.VARCHAR,
    "number": DataType.DOUBLE,
    "enumeration": DataType.ENUM,
    "datetime": DataType.DATETIME,
    "date": DataType.DATE,
    "bool": DataType.BOOLEAN,
    "json": DataType.JSON,
    "phone_number": DataType.VARCHAR,
    "currency": DataType.DOUBLE,
    "html": DataType.VARCHAR,
}


class HubspotSource(DashboardServiceSource):
    @classmethod
    def create(
        cls,
        config_dict: dict,
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,
    ) -> "HubspotSource":
        config = WorkflowSource.model_validate(config_dict)
        connection: HubspotConnection = config.serviceConnection.root.config
        if not isinstance(connection, HubspotConnection):
            raise InvalidSourceException(
                f"Expected HubspotConnection, but got {connection}"
            )
        return cls(config, metadata)

    def close(self) -> None:
        self.client.close()
        super().close()

    def list_datamodels(
        self,
    ) -> Iterable[Union[HubSpotCRMObjectSchema, HubSpotEventDefinition]]:
        yield from self.client.get_crm_object_schemas()
        yield from self.client.get_event_definitions()

    def yield_bulk_datamodel(
        self, model: Union[HubSpotCRMObjectSchema, HubSpotEventDefinition]
    ) -> Iterable[Either[CreateDashboardDataModelRequest]]:
        try:
            if isinstance(model, HubSpotCRMObjectSchema):
                name = model.name
                display_name = model.label
                description = None
                properties = model.properties
            else:
                name = model.name
                display_name = model.label or model.name
                description = Markdown(model.description) if model.description else None
                properties = model.properties

            columns = []
            for prop in properties:
                data_type = HUBSPOT_PROPERTY_TYPE_MAP.get(
                    prop.type or "", DataType.VARCHAR
                )
                col = Column(
                    name=prop.name,
                    displayName=prop.label,
                    description=Markdown(prop.description)
                    if prop.description
                    else None,
                    dataType=data_type,
                    dataLength=65535
                    if data_type
                    in (
                        DataType.VARCHAR,
                        DataType.CHAR,
                        DataType.BINARY,
                        DataType.VARBINARY,
                    )
                    else None,
                )
                columns.append(col)
            datamodel_request = CreateDashboardDataModelRequest(
                name=EntityName(name),
                displayName=display_name,
                description=description,
                dataModelType=DataModelType.HubspotDataModel,
                columns=columns,
                service=self.context.get().dashboard_service,
            )
            yield Either(right=datamodel_request)
            self.register_record_datamodel(datamodel_request=datamodel_request)
        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name=getattr(model, "name", "unknown"),
                    error=f"Error creating data model [{getattr(model, 'name', 'unknown')}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def get_dashboards_list(self) -> Optional[List[HubSpotPipeline]]:
        return self.client.get_pipelines()

    def get_dashboard_name(self, dashboard: HubSpotPipeline) -> str:
        return dashboard.label

    def get_dashboard_details(self, dashboard: HubSpotPipeline) -> HubSpotPipeline:
        return dashboard

    def yield_dashboard(
        self, dashboard_details: HubSpotPipeline
    ) -> Iterable[Either[CreateDashboardRequest]]:
        try:
            hub_id = self.service_connection.hubId or ""
            url_template = PIPELINE_SOURCE_URLS.get(dashboard_details.object_type)
            dashboard_request = CreateDashboardRequest(
                name=EntityName(dashboard_details.id),
                displayName=dashboard_details.label,
                sourceUrl=SourceUrl(url_template.format(hub_id=hub_id))
                if url_template and hub_id
                else None,
                charts=[
                    FullyQualifiedEntityName(
                        fqn.build(
                            self.metadata,
                            entity_type=Chart,
                            service_name=self.context.get().dashboard_service,
                            chart_name=chart,
                        )
                    )
                    for chart in self.context.get().charts or []
                ],
                service=self.context.get().dashboard_service,
            )
            yield Either(right=dashboard_request)
            self.register_record(dashboard_request=dashboard_request)
        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name=dashboard_details.label,
                    error=f"Error creating dashboard [{dashboard_details.label}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def yield_dashboard_chart(
        self, dashboard_details: HubSpotPipeline
    ) -> Iterable[Either[CreateChartRequest]]:
        hub_id = self.service_connection.hubId or ""
        url_template = PIPELINE_SOURCE_URLS.get(dashboard_details.object_type)
        for stage in dashboard_details.stages:
            try:
                if filter_by_chart(self.source_config.chartFilterPattern, stage.label):
                    self.status.filter(stage.label, "Chart Pattern not allowed")
                    continue

                description = None
                if stage.probability is not None:
                    pct = float(stage.probability) * 100
                    description = Markdown(f"Win probability: {pct:.0f}%")

                yield Either(
                    right=CreateChartRequest(
                        name=EntityName(f"{dashboard_details.id}-{stage.id}"),
                        displayName=stage.label,
                        description=description,
                        chartType=ChartType.Other,
                        sourceUrl=SourceUrl(url_template.format(hub_id=hub_id))
                        if url_template and hub_id
                        else None,
                        service=self.context.get().dashboard_service,
                    )
                )
            except Exception as exc:  # pylint: disable=broad-except
                yield Either(
                    left=StackTraceError(
                        name=stage.label,
                        error=f"Error creating chart [{stage.label}]: {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def yield_dashboard_lineage_details(
        self,
        dashboard_details: HubSpotPipeline,
        db_service_prefix: Optional[str] = None,
    ) -> Iterable[Either[AddLineageRequest]]:
        yield from []
