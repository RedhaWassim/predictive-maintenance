from pydantic import BaseModel, model_validator, ConfigDict
import azure
import azure.storage.blob as azure_blob
from src.utils.secrets import get_from_dict_or_env
from typing import Any, Dict, Literal, Optional
from src.utils.logger import get_logger
from src.utils.exceptions import BlobUploadError

class AzureBlobUploader(BaseModel):
    connection_string: str
    container_name: str = "bronze"
    logger: Optional[Any] = get_logger("AzureBlobUploader")

    client: Optional[azure_blob.BlobServiceClient] = None
    container_client: Optional[azure_blob.ContainerClient] = None

    # ✅ FIX: Allow arbitrary types (e.g., azure SDK classes)
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def validate_environement(cls, values: Dict) -> Dict:
        """Validate and set environment variables."""
        values = values.copy()
        values["connection_string"] = get_from_dict_or_env(
            values,
            "connection_string",
            "AZURE_STORAGE_CONNECTION_STRING",
        )
        values["container_name"] = get_from_dict_or_env(
            values,
            "container_name",
            "AZURE_STORAGE_CONTAINER_NAME",
            default="data",
        )
        return values
    

    def model_post_init(self, __context) -> None:
        """Initialize the Azure Blob client after validation."""
        try:
            self.logger.info("INITIALIZING AZURE BLOB UPLOADER")
            self.client = azure_blob.BlobServiceClient.from_connection_string(
            self.connection_string
            )
            self.container_client = self.client.get_container_client(self.container_name)
            self.logger.info("AZURE BLOB UPLOADER INITIALIZED SUCCESSFULLY")
        except Exception as e:
            self.logger.error(f"Blob client initialization error: {str(e)}")
            raise BlobUploadError(f"Failed to initialize blob client: {str(e)}")

    def blob_upload(self,layer: str, local_file: str, blob_name: Optional[str], overwrite: Optional[bool] = True):
        try : 
            self.logger.info(f"UPLOADING {local_file} IN AZURE BLOB as {blob_name}")
            file_name = local_file.split("\\")[-1] or blob_name
            self.container_client = self.client.get_container_client(layer)
            with open(local_file, "rb") as f :
                self.container_client.upload_blob(file_name, f, overwrite = overwrite)
            self.logger.info(f"{local_file} UPLOADED SUCCESSFULLY ")
        except Exception as e : 
            self.logger.error(f"FAILED TO UPLOAD {local_file} error {str(e)}")
            raise BlobUploadError(f"upload failed {str(e)}")
    