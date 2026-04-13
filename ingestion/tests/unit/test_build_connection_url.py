import unittest
from unittest.mock import patch

from azure.core.credentials import AccessToken

from metadata.generated.schema.entity.services.connections.database.azureSQLConnection import (
    Authentication,
    AuthenticationMode,
    AzureSQLConnection,
)
from metadata.generated.schema.entity.services.connections.database.common.azureConfig import (
    AzureConfigurationSource,
)
from metadata.generated.schema.entity.services.connections.database.common.basicAuth import (
    BasicAuth,
)
from metadata.generated.schema.entity.services.connections.database.mysqlConnection import (
    MysqlConnection as MysqlConnectionConfig,
)
from metadata.generated.schema.entity.services.connections.database.postgresConnection import (
    PostgresConnection as PostgresConnectionConfig,
)
from metadata.generated.schema.security.credentials.azureCredentials import (
    AzureCredentials,
)
from metadata.ingestion.source.database.azuresql.connection import get_connection_url
from metadata.ingestion.source.database.mysql.connection import MySQLConnection
from metadata.ingestion.source.database.postgres.connection import PostgresConnection


class TestGetConnectionURL(unittest.TestCase):
    def test_get_connection_url_wo_active_directory_password(self):
        connection = AzureSQLConnection(
            driver="SQL Server",
            hostPort="myserver.database.windows.net",
            database="mydb",
            username="myuser",
            password="mypassword",
            authenticationMode=AuthenticationMode(
                authentication=Authentication.ActiveDirectoryPassword,
                encrypt=True,
                trustServerCertificate=False,
                connectionTimeout=45,
            ),
        )
        expected_url = "mssql+pyodbc://?odbc_connect=Driver%3DSQL+Server%3BServer%3Dmyserver.database.windows.net%3BDatabase%3Dmydb%3BUid%3Dmyuser%3BPwd%3Dmypassword%3BEncrypt%3Dyes%3BTrustServerCertificate%3Dno%3BConnection+Timeout%3D45%3BAuthentication%3DActiveDirectoryPassword%3B"
        self.assertEqual(str(get_connection_url(connection)), expected_url)

        connection = AzureSQLConnection(
            driver="SQL Server",
            hostPort="myserver.database.windows.net",
            database="mydb",
            username="myuser",
            password="mypassword",
            authenticationMode=AuthenticationMode(
                authentication=Authentication.ActiveDirectoryPassword,
            ),
        )

        expected_url = "mssql+pyodbc://?odbc_connect=Driver%3DSQL+Server%3BServer%3Dmyserver.database.windows.net%3BDatabase%3Dmydb%3BUid%3Dmyuser%3BPwd%3Dmypassword%3BEncrypt%3Dno%3BTrustServerCertificate%3Dno%3BConnection+Timeout%3D30%3BAuthentication%3DActiveDirectoryPassword%3B"
        self.assertEqual(str(get_connection_url(connection)), expected_url)

    def test_get_connection_url_mysql(self):
        connection = MysqlConnectionConfig(
            username="openmetadata_user",
            authType=BasicAuth(password="openmetadata_password"),
            hostPort="localhost:3306",
            databaseSchema="openmetadata_db",
        )
        engine_connection = MySQLConnection(connection).client
        self.assertEqual(
            engine_connection.url.render_as_string(hide_password=False),
            "mysql+pymysql://openmetadata_user:openmetadata_password@localhost:3306/openmetadata_db",
        )
        connection = MysqlConnectionConfig(
            username="openmetadata_user",
            authType=AzureConfigurationSource(
                azureConfig=AzureCredentials(
                    clientId="clientid",
                    tenantId="tenantid",
                    clientSecret="clientsecret",
                    scopes="scope1,scope2",
                )
            ),
            hostPort="localhost:3306",
            databaseSchema="openmetadata_db",
        )
        with patch("azure.identity.ClientSecretCredential") as mock_csc:
            mock_instance = mock_csc.return_value
            mock_instance.get_token.return_value = AccessToken(
                token="mocked_token", expires_on=100
            )
            engine_connection = MySQLConnection(connection).client
            mock_csc.assert_called_once_with(
                tenant_id="tenantid",
                client_id="clientid",
                client_secret="clientsecret",
            )
            mock_instance.get_token.assert_called_once_with("scope1", "scope2")
            self.assertEqual(
                engine_connection.url.render_as_string(hide_password=False),
                "mysql+pymysql://openmetadata_user:mocked_token@localhost:3306/openmetadata_db",
            )

    def test_get_connection_url_postgres(self):
        connection = PostgresConnectionConfig(
            username="openmetadata_user",
            authType=BasicAuth(password="openmetadata_password"),
            hostPort="localhost:3306",
            database="openmetadata_db",
        )
        engine_connection = PostgresConnection(connection).client
        self.assertEqual(
            engine_connection.url.render_as_string(hide_password=False),
            "postgresql+psycopg2://openmetadata_user:openmetadata_password@localhost:3306/openmetadata_db",
        )
        connection = PostgresConnectionConfig(
            username="openmetadata_user",
            authType=AzureConfigurationSource(
                azureConfig=AzureCredentials(
                    clientId="clientid",
                    tenantId="tenantid",
                    clientSecret="clientsecret",
                    scopes="scope1,scope2",
                )
            ),
            hostPort="localhost:3306",
            database="openmetadata_db",
        )
        with patch("azure.identity.ClientSecretCredential") as mock_csc:
            mock_instance = mock_csc.return_value
            mock_instance.get_token.return_value = AccessToken(
                token="mocked_token", expires_on=100
            )
            engine_connection = PostgresConnection(connection).client
            mock_csc.assert_called_once_with(
                tenant_id="tenantid",
                client_id="clientid",
                client_secret="clientsecret",
            )
            mock_instance.get_token.assert_called_once_with("scope1", "scope2")
            self.assertEqual(
                engine_connection.url.render_as_string(hide_password=False),
                "postgresql+psycopg2://openmetadata_user:mocked_token@localhost:3306/openmetadata_db",
            )

    def test_get_connection_url_timescale(self):
        from metadata.generated.schema.entity.services.connections.database.timescaleConnection import (
            TimescaleConnection as TimescaleConnectionConfig,
        )
        from metadata.ingestion.source.database.timescale.connection import (
            TimescaleConnection,
        )

        connection = TimescaleConnectionConfig(
            username="openmetadata_user",
            authType=AzureConfigurationSource(
                azureConfig=AzureCredentials(
                    clientId="sentinel-client-id",
                    tenantId="sentinel-tenant-id",
                    clientSecret="sentinel-client-secret",
                    scopes="https://sentinel.scope/.default",
                )
            ),
            hostPort="localhost:5432",
            database="openmetadata_db",
        )
        with patch("azure.identity.ClientSecretCredential") as mock_csc:
            mock_instance = mock_csc.return_value
            mock_instance.get_token.return_value = AccessToken(
                token="sentinel_token", expires_on=100
            )
            engine = TimescaleConnection(connection).client
            mock_csc.assert_called_once_with(
                tenant_id="sentinel-tenant-id",
                client_id="sentinel-client-id",
                client_secret="sentinel-client-secret",
            )
            mock_instance.get_token.assert_called_once_with(
                "https://sentinel.scope/.default"
            )
            self.assertIn(
                "sentinel_token",
                engine.url.render_as_string(hide_password=False),
            )
