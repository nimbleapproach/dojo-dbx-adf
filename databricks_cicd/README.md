# Azure DevOps Pipeline for Databricks Deployment

This pipeline automates the deployment of notebooks and jobs to Databricks using the Databricks CLI. It includes steps for setting up the environment, validating the build, and deploying assets to Databricks.

## Trigger Configuration 👉
- **Branches**: The pipeline triggers on changes to `main`, `uat`, and `prod` branches.
- **Pull Requests**: It also triggers for pull requests made against these branches.

## Agent Pool 🕵️
- **Pool**: The jobs run on the `Azure Pipelines` agent pool.

## Variables 📊
- **Environment Variables**: Defined for deployment environments (`main`, `uat`, `prod`), Key Vault names, and Azure subscriptions.
- **Display Name**: Changes based on whether the build is for a pull request or a merge.

## Jobs 👷‍♂️
### Deploy Databricks Workflow
1. **Fetch Secrets**: Retrieves secrets from Azure Key Vault.
2. **Download Databricks CLI**: Downloads the Databricks CLI package from Azure DevOps Artifacts.
3. **Setup Environment**: Configures environment variables and makes the Databricks binary executable.
4. **Validate and Deploy**:
   - **Validation**: Validates the Databricks bundle.
   - **Deployment**: Deploys the Databricks bundle and DDLs if the build is not triggered by a pull request.

### Conditional Deployment to Production
- **Deploy to Prod**: Deploys to the production environment if the source branch is `prod` and the build succeeds.

## Scripts 📜
- **Setup Environment**: Sets up necessary environment variables.
- **Validation**: Validates the Databricks bundle.
- **Deployment**: Deploys the Databricks bundle and DDLs based on the branch or pull request status.

## Repository structure 🏗️

### 📂initial_build
The `initial_build` folder is designated for setting up DML and DDL across all catalogues, schemas, and tables. To prevent files from being executed multiple times, it is monitored via the `ddl_deployment` table. This table records the checksum value of each file during every release, ensuring that each file is only run once.

### 📂scripts
Contains various scripts used in the deployment process.

### 📂notebooks
Stores the Databricks notebooks to be deployed. these are called by workflows/jobs with the ETL

### 📂resources
Here is where the workflows/jobs are created. if you run the deployment through the `manual` deployment a copy of each of these jobs will be create under your dev workspace eg `[dev yourname] databrick_cicd_job`. these can be kicked off manually within the WEB IDE and are referencing you user workspace

### 📂test
These python files run as a job as part of the CICD deployment process to ensure the functional and unit tests are carried out before deploying. this folder is triggered via the `run_test.py` file


## Getting started 🚀

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```powershell
    $ databricks configure
    ```
4. Configure your enviornment settings:
    ```powershell
    databricks auth env --profile <profile-name>
    ```

    this will add your config in the `.databrickscfg` file. this file can be found using scripts below

    ```powershell
    cd $env:USERPROFILE
    ## List file in Dir
    dir .databrickscfg /a
    ## Open the file in your Editor
    code .databrickscfg
    ```
3. To validate a manual copy of this project, type:
    ```powershell
    $ databricks bundle validate --target manual --profile <profile-name>
    ```
    (Note that "manual" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] databrick_cicd_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/databrick_cicd_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

4. To deploy a development use the PR process to push the changes through:
    https://dev.azure.com/InfinigateHolding/Group%20IT%20Program/_git/inf-databricks/pullrequests?_a=mine
    

5. Similarly, to deploy a uat or production use the PR process and merge into the prod branch to push the changes through:
   https://dev.azure.com/InfinigateHolding/Group%20IT%20Program/_git/inf-databricks/pullrequests?_a=mine

5. To run a job or pipeline, use the "run" command:
   ```powershell
   $ databricks bundle run <job-name>
   ```
5. To run a job or pipeline from a manual branch, use the "run" command:
   ```powershell
   $ databricks bundle run --target manual jobs.inf_edw_model_job_deploy_ddl --profile <profile-name>
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.



## FAQ 🚀

### How to clear down Asset Bundles
1. To destroy your dev Bundle/Workspace run the following:
   ```powershell
   databricks bundle destroy --target manual --profile dev
   ```
