from pathlib import Path
from DataManager import DataManager
from Configuration import AuditConfig


class AuditInitialiser:
    """
    Initialises the audit environment for a specific tenant.

    This class handles the creation of the necessary directory structure for a tenant
    and copies the default configuration files to the new location.
    """

    CONFIG_FILES = [
        AuditConfig.TENANT_CONFIG_ENVIRONMENTS_PATH,
        AuditConfig.TENANT_CONFIG_EXCLUSIONS_PATH,
        AuditConfig.TENANT_CONFIG_REGIONS_PATH
    ]

    def __init__(self, output_base: str, tenant: str, data_manager: DataManager):
        """
        Initialises the audit environment for a given tenant.

        Args:
            output_base: The base directory where tenant data will be stored.
            tenant: The name of the tenant.
            data_manager: An instance of the DataManager for file operations.
        """
        self.output_base = output_base
        self.tenant = tenant
        self.dm = data_manager

        self.DEFAULT_CONFIG_PATH = Path(__file__).parent / "DefaultTenantConfig"
        self.tenant_path = Path(self.output_base) / self.tenant
        self.tenant_config_path = self.tenant_path / f"{self.tenant}Config"

        self._initialise_tenant()

    def _initialise_tenant(self):
        """
        Creates the tenant directory and copies the default configuration files.
        """
        print(f"Initialising tenant: {self.tenant}")
        print(f"Tenant config path: {self.tenant_config_path}")

        # The copy_file method will create the destination directory if it doesn't exist.
        for config_file in self.CONFIG_FILES:
            source_path = self.DEFAULT_CONFIG_PATH / config_file
            dest_path = self.tenant_config_path / config_file

            if not self.dm.fs.exists(dest_path):
                print(f"Copying {config_file} to {self.tenant_config_path}")
                self.dm.copy_file(source_path, dest_path)
            else:
                print(f"{config_file} already exists in {self.tenant_config_path}. Skipping.")

        print(f"Tenant {self.tenant} initialised successfully.")
