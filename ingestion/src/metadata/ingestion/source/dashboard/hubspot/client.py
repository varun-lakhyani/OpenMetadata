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
HubSpot API client wrapper
"""
import time
import traceback
from typing import Dict, List, Optional, Union

import requests
from hubspot import HubSpot
from requests import HTTPError

from metadata.generated.schema.entity.services.connections.dashboard.hubspotConnection import (
    HubspotConnection,
)
from metadata.ingestion.source.dashboard.hubspot.models import (
    HubSpotCRMObjectSchema,
    HubSpotCRMProperty,
    HubSpotEventDefinition,
    HubSpotEventProperty,
    HubSpotObjectTypeInfo,
    HubSpotPipeline,
    HubSpotPipelineStage,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

# Standard HubSpot object types not returned by /crm/v3/schemas (which lists custom objects only).
STANDARD_CRM_OBJECT_TYPES = [
    "contacts",
    "companies",
    "deals",
    "tickets",
    "orders",
    "products",
    "line_items",
    "quotes",
]

STANDARD_CRM_LABELS: Dict[str, str] = {
    "contacts": "Contacts",
    "companies": "Companies",
    "deals": "Deals",
    "tickets": "Tickets",
    "orders": "Orders",
    "products": "Products",
    "line_items": "Line Items",
    "quotes": "Quotes",
}

STANDARD_PIPELINE_OBJECT_TYPES = ["deals", "tickets", "orders"]


class HubSpotClient:
    """
    Wraps the HubSpot SDK and exposes methods needed for metadata ingestion.

    Entity mapping:
      - CRM Pipeline               → Dashboard
      - Pipeline Stage             → Chart
      - CRM Object Schema          → DashboardDataModel
      - Custom Event Definition    → DashboardDataModel
    """

    _BASE_URL = "https://api.hubapi.com"

    def __init__(self, connection: HubspotConnection) -> None:
        self._access_token = connection.accessToken.get_secret_value()
        self._api = HubSpot(access_token=self._access_token)
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {self._access_token}"
        self._custom_object_type_infos: Optional[List[HubSpotObjectTypeInfo]] = None

    def close(self) -> None:
        self._session.close()

    def _get(
        self, path: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> dict:
        """Authenticated GET for endpoints not yet wrapped by the HubSpot SDK."""
        response = None
        for attempt in range(3):
            response = self._session.get(
                f"{self._BASE_URL}{path}",
                params=params or {},
                timeout=30,
            )
            if response.status_code == 429:
                try:
                    retry_after = int(response.headers.get("Retry-After", 10))
                except ValueError:
                    retry_after = 10
                logger.warning(
                    "HubSpot rate limit hit, retrying after %ss (attempt %s/3)",
                    retry_after,
                    attempt + 1,
                )
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            try:
                return response.json()
            except ValueError as exc:
                raise ValueError(
                    f"Invalid JSON response from {path}: {response.text[:200]}"
                ) from exc
        raise RuntimeError(f"HubSpot rate limit exceeded after 3 retries for {path}")

    @staticmethod
    def _is_scope_error(exc: Exception) -> bool:
        if isinstance(exc, HTTPError):
            return exc.response is not None and exc.response.status_code == 403
        # HubSpot SDK raises ApiException with a status attribute
        return getattr(exc, "status", None) == 403

    def check_access(self) -> None:
        self._api.crm.owners.owners_api.get_page(limit=1)

    def get_pipelines_sample(self) -> None:
        self._get("/crm/v3/pipelines/deals", {"limit": 1})

    def get_crm_schemas_sample(self) -> None:
        self._get("/crm/v3/properties/contacts", {"limit": 1})

    def get_event_definitions_sample(self) -> None:
        self._get("/events/v3/event-definitions", {"limit": 1})

    def _get_custom_object_type_infos(self) -> List[HubSpotObjectTypeInfo]:
        """
        Fetch all custom object schemas from /crm/v3/schemas (cached per client instance).
        Returns lightweight descriptors including name and hasPipeline flag.
        """
        if self._custom_object_type_infos is not None:
            return self._custom_object_type_infos
        infos = []
        try:
            after = None
            while True:
                params: Dict[str, Union[str, int]] = {"limit": 100}
                if after:
                    params["after"] = after
                data = self._get("/crm/v3/schemas", params)
                for item in data.get("results", []):
                    infos.append(
                        HubSpotObjectTypeInfo(
                            name=item["name"],
                            objectTypeId=item.get("objectTypeId"),
                            labels=item.get("labels"),
                            hasPipeline=item.get("hasPipeline", False),
                        )
                    )
                after = data.get("paging", {}).get("next", {}).get("after")
                if not after:
                    break
            self._custom_object_type_infos = infos
        except Exception as exc:
            logger.debug(traceback.format_exc())
            if self._is_scope_error(exc):
                logger.warning(
                    "Skipping custom object schema discovery: token lacks crm.schemas.custom.read scope"
                )
            else:
                logger.warning("Failed to fetch custom object schemas: %s", exc)
            self._custom_object_type_infos = infos
        return infos

    def _get_pipeline_object_types(self) -> List[str]:
        """
        Build the full list of object types that support pipelines:
        standard pipeline-capable types + any custom objects with hasPipeline=True.
        """
        custom_with_pipeline = [
            info.name
            for info in self._get_custom_object_type_infos()
            if info.has_pipeline
        ]
        return STANDARD_PIPELINE_OBJECT_TYPES + custom_with_pipeline

    def _get_all_crm_object_types(self) -> List[str]:
        """
        Build the full list of CRM object types to fetch schemas for:
        standard types + all custom object types.
        """
        custom_types = [info.name for info in self._get_custom_object_type_infos()]
        seen = set(STANDARD_CRM_OBJECT_TYPES)
        extra = [t for t in custom_types if t not in seen]
        return STANDARD_CRM_OBJECT_TYPES + extra

    def get_pipelines(self) -> List[HubSpotPipeline]:
        pipelines = []
        for object_type in self._get_pipeline_object_types():
            try:
                result = self._api.crm.pipelines.pipelines_api.get_all(
                    object_type=object_type
                )
                for pipeline in result.results or []:
                    if pipeline.archived:
                        continue
                    stages = [
                        HubSpotPipelineStage(
                            id=stage.id,
                            label=stage.label,
                            displayOrder=stage.display_order,
                            probability=stage.metadata.get("probability")
                            if stage.metadata
                            else None,
                            isClosed=stage.metadata.get("isClosed") == "true"
                            if stage.metadata
                            else None,
                        )
                        for stage in sorted(
                            pipeline.stages or [],
                            key=lambda s: s.display_order or 0,
                        )
                    ]
                    pipelines.append(
                        HubSpotPipeline(
                            id=pipeline.id,
                            label=pipeline.label,
                            displayOrder=pipeline.display_order,
                            archived=pipeline.archived,
                            stages=stages,
                            objectType=object_type,
                        )
                    )
            except Exception as exc:
                logger.debug(traceback.format_exc())
                if self._is_scope_error(exc):
                    logger.warning(
                        "Skipping %s pipelines: token lacks required pipeline scope",
                        object_type,
                    )
                else:
                    logger.warning("Failed to fetch %s pipelines: %s", object_type, exc)
        return pipelines

    def get_event_definitions(self) -> List[HubSpotEventDefinition]:
        """
        Fetch all custom behavioral event definitions via
        GET /events/v3/event-definitions
        Requires scope: behavioral_events.event_definitions.read_write
        """
        event_definitions = []
        try:
            after = None
            while True:
                params: Dict[str, Union[str, int]] = {
                    "includeProperties": "true",
                    "limit": 100,
                }
                if after:
                    params["after"] = after
                data = self._get("/events/v3/event-definitions", params)
                for item in data.get("results", []):
                    properties = [
                        HubSpotEventProperty(
                            name=prop.get("name", ""),
                            label=prop.get("label", prop.get("name", "")),
                            type=prop.get("type"),
                            description=prop.get("description"),
                        )
                        for prop in item.get("propertyDefinitions", [])
                    ]
                    event_definitions.append(
                        HubSpotEventDefinition(
                            name=item["name"],
                            label=item.get("label"),
                            description=item.get("description"),
                            primaryObject=item.get("primaryObject"),
                            properties=properties,
                        )
                    )
                after = data.get("paging", {}).get("next", {}).get("after")
                if not after:
                    break
        except Exception as exc:
            logger.debug(traceback.format_exc())
            if self._is_scope_error(exc):
                logger.warning(
                    "Skipping event definitions: token lacks behavioral_events.event_definitions.read_write scope"
                )
            else:
                logger.warning("Failed to fetch event definitions: %s", exc)
        return event_definitions

    def get_crm_object_schemas(self) -> List[HubSpotCRMObjectSchema]:
        """
        Fetch property schemas for all CRM object types: standard ones (contacts, companies,
        deals, tickets, orders, products, line_items, quotes) plus any custom objects discovered
        via /crm/v3/schemas. Each becomes a DashboardDataModel with its properties as columns.
        """
        schemas = []
        for object_type in self._get_all_crm_object_types():
            try:
                properties = []
                after = None
                while True:
                    params: Dict[str, Union[str, int]] = {"limit": 100}
                    if after:
                        params["after"] = after
                    data = self._get(f"/crm/v3/properties/{object_type}", params)
                    properties.extend(
                        HubSpotCRMProperty(
                            name=prop["name"],
                            label=prop.get("label", prop["name"]),
                            type=prop.get("type"),
                            description=prop.get("description") or None,
                            archived=prop.get("archived", False),
                            hidden=prop.get("hidden", False),
                        )
                        for prop in data.get("results", [])
                        if not prop.get("archived") and not prop.get("hidden")
                    )
                    after = data.get("paging", {}).get("next", {}).get("after")
                    if not after:
                        break
                label = STANDARD_CRM_LABELS.get(
                    object_type, object_type.replace("_", " ").title()
                )
                schemas.append(
                    HubSpotCRMObjectSchema(
                        name=object_type,
                        label=label,
                        properties=properties,
                    )
                )
            except Exception as exc:
                logger.debug(traceback.format_exc())
                if self._is_scope_error(exc):
                    logger.warning(
                        "Skipping %s schema: token lacks required read scope",
                        object_type,
                    )
                else:
                    logger.warning(
                        "Failed to fetch %s property schema: %s", object_type, exc
                    )
        return schemas
