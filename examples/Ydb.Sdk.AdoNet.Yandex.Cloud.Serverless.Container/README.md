# YDB Sdk ADO.NET for Serverless Containers in Yandex Cloud

Sample application uses the YDB SDK for ADO.NET and can be deployed
to [Yandex Cloud Serverless Containers](https://yandex.cloud/en/docs/serverless-containers/).

## Getting started

1. **Setup
   ** [Yandex Container Registry](https://yandex.cloud/en/docs/container-registry/operations/registry/registry-create).
2. **Build and Push Docker Image**
   ```bash 
   docker build . -t cr.yandex/<container-registry-id>/ado-net-app:latest
   docker push cr.yandex/<container-registry-id>/ado-net-app:latest
   ```
   Replace <container-registry-id> with your actual Container Registry ID.
3. **Grant Required Permissions**. To enable your Serverless Container to access both YDB and your container image in
   the Container Registry, grant the following roles to your Service Account:

    - `ydb.editor` — access to YDB,
    - `container-registry.images.puller` — permission to pull images from Container Registry.

4. **Create a new revision**. After pushing your image, create a new version of the Serverless Container as described in
   the [official guide](https://yandex.cloud/en/docs/serverless-containers/quickstart/container#create-revision).
   Specify your image and the necessary environment variables and secrets.

5. **Running the Yandex Serverless Container**.
   After the new revision has been rolled out, you can use your container (e.g., for a health check) by executing the
   following command:
   ```bash
   curl --header "Authorization: Bearer $(yc iam create-token)" https://<serverless-container-host>/
   ```
   Replace <serverless-container-host> with your actual serverless container host.

## ⚠️ Important Note: YdbDataSource and Connection Handling

When using the YDB SDK in a serverless container, it is always necessary to create a new YdbDataSource for each request
or function invocation.

This is because the network connectivity between a serverless container and external resources, such as YDB, can be
interrupted or reset during execution. If you cache or reuse existing connections, you may experience failures due to
stale or invalid connections.

Best practice:

_Create a fresh data source (or Driver for Topic API) for every request or function invocation in order to ensure
reliability in the serverless environment._
