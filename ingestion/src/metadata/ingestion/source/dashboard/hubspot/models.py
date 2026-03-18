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
HubSpot API response models
"""
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class HubSpotPipelineStage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    label: str
    display_order: Optional[int] = Field(None, alias="displayOrder")
    probability: Optional[str] = None
    is_closed: Optional[bool] = Field(None, alias="isClosed")


class HubSpotPipeline(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    label: str
    display_order: Optional[int] = Field(None, alias="displayOrder")
    archived: Optional[bool] = None
    stages: List[HubSpotPipelineStage] = []
    object_type: Optional[str] = Field(None, alias="objectType")


class HubSpotEventProperty(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    label: str
    type: Optional[str] = None
    description: Optional[str] = None


class HubSpotEventDefinition(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    primary_object: Optional[str] = Field(None, alias="primaryObject")
    properties: List[HubSpotEventProperty] = []


class HubSpotCRMProperty(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    label: str
    type: Optional[str] = None
    description: Optional[str] = None
    archived: Optional[bool] = None
    hidden: Optional[bool] = None


class HubSpotCRMObjectSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    label: str
    properties: List[HubSpotCRMProperty] = []


class HubSpotObjectTypeInfo(BaseModel):
    """Lightweight descriptor returned by /crm/v3/schemas for each object type."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    object_type_id: Optional[str] = Field(None, alias="objectTypeId")
    labels: Optional[dict] = None
    has_pipeline: Optional[bool] = Field(None, alias="hasPipeline")
