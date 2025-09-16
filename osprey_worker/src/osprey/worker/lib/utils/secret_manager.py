from google.cloud import secretmanager


def fetch_secret(project_id: str, name: str, version: str = 'latest') -> str:
    gsm_client = secretmanager.SecretManagerServiceClient()
    secret_reference = gsm_client.secret_version_path(project=project_id, secret=name, secret_version=version)
    response = gsm_client.access_secret_version(request={'name': secret_reference})
    return response.payload.data.decode()
