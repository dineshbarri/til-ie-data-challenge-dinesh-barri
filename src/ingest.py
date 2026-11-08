
import os
from pathlib import Path
from azure.storage.blob import ContainerClient

ACCOUNT = "sttiliedatachallenge"
CONTAINER = "til-ie-data-challenge"
DESTINATION = Path("src/data/raw")


def download_source_data():
    sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN")

    if not sas_token:
        raise RuntimeError(
            "AZURE_STORAGE_SAS_TOKEN is not configured"
        )

    client = ContainerClient(
        account_url=f"https://{ACCOUNT}.blob.core.windows.net",
        container_name=CONTAINER,
        credential=sas_token.lstrip("?"),
    )

    downloaded = []

    for prefix in ("sales/", "rebates/"):
        for blob in client.list_blobs(name_starts_with=prefix):
            if not blob.name.lower().endswith(".csv"):
                continue

            destination = DESTINATION / blob.name
            destination.parent.mkdir(
                parents=True, exist_ok=True
            )

            if (
                destination.exists()
                and destination.stat().st_size == blob.size
            ):
                print(f"Already present: {blob.name}")
                continue

            temporary = destination.with_suffix(".csv.part")

            try:
                with temporary.open("wb") as output:
                    stream = client.download_blob(blob.name)
                    stream.readinto(output)

                if temporary.stat().st_size != blob.size:
                    raise IOError(
                        f"Incomplete download: {blob.name}"
                    )

                temporary.replace(destination)
                downloaded.append(blob.name)

            finally:
                temporary.unlink(missing_ok=True)

            print(f"Downloaded: {blob.name}")

    return downloaded


if __name__ == "__main__":
    files = download_source_data()
    print(f"New files downloaded: {len(files)}")
